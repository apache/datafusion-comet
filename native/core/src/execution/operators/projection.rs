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

use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::Operator;
use jni::objects::GlobalRef;

use crate::{
    execution::{
        operators::{ExecutionError, ScanExec},
        planner::{operator_registry::OperatorBuilder, PhysicalPlanner},
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
        inputs: &mut Vec<Arc<GlobalRef>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> Result<(Vec<ScanExec>, Arc<SparkPlan>), ExecutionError> {
        let project = extract_op!(spark_plan, Projection);
        let children = &spark_plan.children;

        assert_eq!(children.len(), 1);
        let (scans, child) = planner.create_plan(&children[0], inputs, partition_count)?;

        // Create projection expressions
        let exprs: Result<Vec<_>, _> = project
            .project_list
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                planner
                    .create_expr(expr, child.schema())
                    .map(|r| (r, format!("col_{idx}")))
            })
            .collect();

        let projection = Arc::new(ProjectionExec::try_new(
            exprs?,
            Arc::clone(&child.native_plan),
        )?);

        Ok((
            scans,
            Arc::new(SparkPlan::new(spark_plan.plan_id, projection, vec![child])),
        ))
    }
}
