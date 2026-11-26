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

//! Builder for the native `Explode` operator, which plans Spark's `explode`, `posexplode`, and
//! their `_outer` variants as a projection followed by Comet's `ExplodeExec`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    common::{NullHandling, UnnestOptions},
    physical_expr::{expressions::Column, PhysicalExpr},
    physical_plan::{projection::ProjectionExec, unnest::ListUnnest, ExecutionPlan},
};
use datafusion_comet_operators::ExplodeExec;
use datafusion_comet_proto::spark_operator::Operator;
use datafusion_comet_spark_expr::ListPositionsExpr;
use jni::objects::{Global, JObject};

use super::{operator_registry::OperatorBuilder, PhysicalPlanner, PlanCreationResult};
use crate::{
    execution::{
        expressions::map_entries::MapEntriesExpr, operators::ExecutionError, spark_plan::SparkPlan,
    },
    extract_op,
};

/// Builder for `Explode`.
pub struct ExplodeBuilder;

impl OperatorBuilder for ExplodeBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let explode = extract_op!(spark_plan, Explode);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;

        // Create the expression for the collection to explode
        let child_expr = if let Some(child_expr) = &explode.child {
            planner.create_expr(child_expr, child.schema())?
        } else {
            return Err(ExecutionError::GeneralError(
                "Explode operator requires a child expression".to_string(),
            ));
        };

        let child_schema = child.schema();
        let child_field_name = child_expr
            .return_field(&child_schema)
            .expect("Failed to get field from child expression")
            .name()
            .to_string();

        // Expose maps as List<Struct<key, value>>, sharing their entries buffers.
        // Reuse the list path for outer rows, positions and bounded output batches,
        // then flatten the entry struct into Spark's two output columns.
        let map_fields = match child_expr.data_type(&child_schema)? {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) => Some(fields.clone()),
                _ => {
                    return Err(ExecutionError::GeneralError(
                        "Map entries must be a struct".to_string(),
                    ));
                }
            },
            _ => None,
        };
        let child_expr: Arc<dyn PhysicalExpr> = if map_fields.is_some() {
            Arc::new(MapEntriesExpr::new(child_expr))
        } else {
            child_expr
        };

        // Both posexplode variants reference the array twice: once for positions
        // and once for values. Materialize computed arrays so both references
        // share one evaluation. A plain Column is already materialized.
        let (child_expr, child_native_plan) = if explode.position && !child_expr.is::<Column>() {
            let reserved_name = format!("__comet_explode_{}", child_field_name);
            let mut pre_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = child_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    (
                        Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                        f.name().to_string(),
                    )
                })
                .collect();
            let child_idx = pre_exprs.len();
            pre_exprs.push((child_expr, reserved_name.clone()));

            let pre_exec = Arc::new(ProjectionExec::try_new(
                pre_exprs,
                Arc::clone(&child.native_plan),
            )?);
            (
                Arc::new(Column::new(&reserved_name, child_idx)) as Arc<dyn PhysicalExpr>,
                pre_exec as Arc<dyn ExecutionPlan>,
            )
        } else {
            (child_expr, Arc::clone(&child.native_plan))
        };

        // Create projection expressions for other columns
        let projections: Vec<Arc<dyn PhysicalExpr>> = explode
            .project_list
            .iter()
            .map(|expr| planner.create_expr(expr, child.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        // For posexplode, a parallel List<Int32> positions column is added before the
        // array column so UnnestExec can unnest both in parallel.
        let mut project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projections
            .iter()
            .map(|expr| {
                let field = expr
                    .return_field(&child_schema)
                    .expect("Failed to get field from expression");
                let name = field.name().to_string();
                (Arc::clone(expr), name)
            })
            .collect();

        if explode.position {
            let positions_expr: Arc<dyn PhysicalExpr> =
                Arc::new(ListPositionsExpr::new(Arc::clone(&child_expr)));
            project_exprs.push((positions_expr, "pos".to_string()));
        }
        project_exprs.push((Arc::clone(&child_expr), child_field_name.clone()));

        let project_exec = Arc::new(ProjectionExec::try_new(project_exprs, child_native_plan)?);

        let project_schema = project_exec.schema();

        // Build the output schema for UnnestExec
        let mut output_fields: Vec<Field> = Vec::new();

        // Add all projection columns (non-array columns)
        for i in 0..projections.len() {
            output_fields.push(project_schema.field(i).clone());
        }

        let array_input_index = if explode.position {
            // With outer=true, UnnestExec preserves rows whose array is empty or NULL
            // and emits a NULL position for them, so pos must be nullable in that case.
            output_fields.push(Field::new("pos", DataType::Int32, explode.outer));
            projections.len() + 1
        } else {
            projections.len()
        };

        // Extract the element type from the list/array type
        let array_field = project_schema.field(array_input_index);
        let element_type = match array_field.data_type() {
            DataType::List(field) => field.data_type().clone(),
            dt => {
                return Err(ExecutionError::GeneralError(format!(
                    "Expected List type for explode, got {:?}",
                    dt
                )))
            }
        };

        let struct_unnests = if let Some(fields) = map_fields {
            output_fields.extend(fields.iter().map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_nullable(explode.outer || field.is_nullable())
            }));
            vec![array_input_index]
        } else {
            output_fields.push(Field::new(
                array_field.name(),
                element_type,
                true, // Element is nullable after unnesting
            ));
            vec![]
        };

        let output_schema = Arc::new(Schema::new(output_fields));

        let mut list_unnests = Vec::with_capacity(2);
        if explode.position {
            list_unnests.push(ListUnnest {
                index_in_input_schema: projections.len(),
                depth: 1,
            });
        }
        list_unnests.push(ListUnnest {
            index_in_input_schema: array_input_index,
            depth: 1,
        });

        // Spark's `explode_outer`/`posexplode_outer` emit exactly one null row for both
        // a NULL array and an empty one, which is `PreserveAndExpandEmpty`. Plain
        // `explode` drops both.
        let null_handling = if explode.outer {
            NullHandling::PreserveAndExpandEmpty
        } else {
            NullHandling::Drop
        };
        let unnest_options = UnnestOptions::new().with_null_handling(null_handling);

        // Comet's specialized fork of `UnnestExec`; see `operators::explode`.
        let unnest_exec = Arc::new(ExplodeExec::new(
            project_exec,
            list_unnests,
            struct_unnests,
            output_schema,
            unnest_options,
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(spark_plan.plan_id, unnest_exec, vec![child])),
        ))
    }
}
