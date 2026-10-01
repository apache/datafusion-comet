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

//! Local graph construction. Reuse Comet scan/expression builders without per-task plan execution.

use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{
    filter::FilterExec, projection::ProjectionExec, union::UnionExec, ExecutionPlan,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_comet_local::LocalQuery;
use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator, SparkFilePartition};
use prost::Message;

use crate::execution::operators::ExecutionError;
use crate::execution::planner::PhysicalPlanner;
use crate::parquet::parquet_support::CometObjectStoreRegistry;

pub(super) fn parquet_query(
    bytes: &[u8],
    partitions: &[Vec<u8>],
    batch_size: usize,
    columns: usize,
    row_filter_pushdown: bool,
) -> Result<LocalQuery, ExecutionError> {
    let root = Operator::decode(bytes)?;
    let groups = partitions
        .iter()
        .map(|b| SparkFilePartition::decode(b.as_slice()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(groups.len().max(1));
    config.options_mut().execution.parquet.pushdown_filters = row_filter_pushdown;
    config.options_mut().execution.parquet.reorder_filters = row_filter_pushdown;
    // Registry and configuration are query-owned. Never inherit another query's credentials.
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(CometObjectStoreRegistry::default()))
        .build()?;
    let context = Arc::new(SessionContext::new_with_config_rt(
        config,
        Arc::new(runtime),
    ));
    let planner = PhysicalPlanner::new(Arc::clone(&context), 0).with_sql_text_pool(&root);
    let plan = build(&root, &groups, &planner)?;
    if plan.schema().fields().len() != columns {
        return Err(ExecutionError::GeneralError(
            "Local output schema width mismatch".into(),
        ));
    }
    Ok(LocalQuery::new(plan, context.task_ctx()))
}

fn build(
    op: &Operator,
    groups: &[SparkFilePartition],
    planner: &PhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    let invalid = || ExecutionError::GeneralError("Invalid local Parquet plan".into());
    match op.op_struct.as_ref().ok_or_else(invalid)? {
        OpStruct::NativeScan(scan) if op.children.is_empty() => {
            let common = scan.common.as_ref().ok_or_else(invalid)?;
            if common.encryption_enabled || !common.object_store_options.is_empty() {
                return Err(ExecutionError::GeneralError(
                    "Local scan requires unencrypted local files".into(),
                ));
            }
            let empty = SparkFilePartition::default();
            let groups = if groups.is_empty() {
                std::slice::from_ref(&empty)
            } else {
                groups
            };
            let mut plans = Vec::with_capacity(groups.len());
            for group in groups {
                if group.partitioned_file.iter().any(|file| {
                    url::Url::parse(&file.file_path).map_or(true, |url| url.scheme() != "file")
                }) {
                    return Err(invalid());
                }
                let mut scan = scan.clone();
                scan.file_partition = Some(group.clone());
                let mut leaf = op.clone();
                leaf.op_struct = Some(OpStruct::NativeScan(scan));
                let (inputs, shuffles, plan) = planner.create_plan(&leaf, &mut vec![], 1)?;
                if !inputs.is_empty() || !shuffles.is_empty() {
                    return Err(invalid());
                }
                plans.push(Arc::clone(&plan.native_plan));
            }
            if plans.len() == 1 {
                Ok(plans.remove(0))
            } else {
                Ok(UnionExec::try_new(plans)?)
            }
        }
        OpStruct::Projection(project)
            if op.children.len() == 1 && !project.project_list.is_empty() =>
        {
            let child = build(&op.children[0], groups, planner)?;
            let expressions = project
                .project_list
                .iter()
                .enumerate()
                .map(|(i, expression)| {
                    planner
                        .create_expr(expression, child.schema())
                        .map(|e| (e, format!("col_{i}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(ProjectionExec::try_new(expressions, child)?))
        }
        OpStruct::Filter(filter) if op.children.len() == 1 => {
            let child = build(&op.children[0], groups, planner)?;
            let predicate = planner.create_expr(
                filter.predicate.as_ref().ok_or_else(invalid)?,
                child.schema(),
            )?;
            Ok(Arc::new(FilterExec::try_new(predicate, child)?))
        }
        _ => Err(invalid()),
    }
}
