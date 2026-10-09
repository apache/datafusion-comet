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

//! Projection operator builder

use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::Operator;
use jni::objects::{Global, JObject};

use crate::{
    execution::{
        planner::{operator_registry::OperatorBuilder, PhysicalPlanner, PlanCreationResult},
        spark_plan::SparkPlan,
    },
    extract_op,
};

/// Builder for Projection operators
pub struct ProjectionBuilder;

impl OperatorBuilder for ProjectionBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let project = extract_op!(spark_plan, Projection);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, mut child) =
            planner.create_plan(&children[0], inputs, partition_count)?;

        // Create projection expressions
        let exprs: Result<Vec<_>, _> = project
            .project_list
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                planner
                    .create_expr(expr, child.schema())
                    .map(|r| ProjectionExpr::new(r, format!("col_{idx}")))
            })
            .collect();

        let mut exprs = ProjectionExprs::from(exprs?);
        if let Some(filter) = child.native_plan.downcast_ref::<FilterExec>() {
            if exprs.iter().all(|expr| expr.expr.is::<Column>()) {
                let indices = exprs.column_indices();
                if indices.len() < child.schema().fields().len() {
                    // Filter each required output column once, then restore its order and aliases.
                    // Keep both native plans so Spark metrics describe the executed work.
                    let mapping = ProjectionExprs::from_indices(&indices, &child.schema());
                    exprs = exprs.try_map_exprs(|expr| mapping.project_expr(&expr))?;
                    let filter = FilterExecBuilder::from(filter)
                        .apply_projection(Some(indices))?
                        .build()?;
                    Arc::make_mut(&mut child).native_plan = Arc::new(filter);
                }
            }
        }
        let projection = Arc::new(ProjectionExec::try_new(
            exprs.iter().cloned(),
            Arc::clone(&child.native_plan),
        )?);

        let spark_plan = if child.plan_id == spark_plan.plan_id {
            let mut additional_native_plans = vec![Arc::clone(&child.native_plan)];
            additional_native_plans.extend(child.additional_native_plans.iter().cloned());
            Arc::new(SparkPlan::new_with_additional(
                spark_plan.plan_id,
                projection,
                child.children.clone(),
                additional_native_plans,
            ))
        } else {
            Arc::new(SparkPlan::new(spark_plan.plan_id, projection, vec![child]))
        };

        Ok((scans, shuffle_scans, spark_plan))
    }
}
