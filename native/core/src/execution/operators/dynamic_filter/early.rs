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

//! Place a live ancestor filter before an intermediate join's probe work.
//!
//! This is decoded-batch filtering only. It leaves scan/schema conversion and
//! arbitrary expressions in place, and never propagates into an intermediate
//! build side. The downstream join remains the authority for matching rows.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{internal_err, Result};
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::CometFilterExec;

use super::parquet_reader::is_direct_column_null_checks;
use super::{DynamicFilterExec, DynamicFilterJoinExec};
use crate::execution::operators::CometProjectionExec;

pub(super) fn place_early_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(place(input, predicate, metrics, false)?.unwrap_or_else(|| Arc::clone(input)))
}

fn remap(
    predicate: Arc<DynamicFilterPhysicalExpr>,
    column: Arc<dyn PhysicalExpr>,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    // Derived expressions share producer updates. Taking current() here would
    // capture the initial TRUE placeholder instead of the completed build domain.
    let mapped: Arc<dyn Any + Send + Sync> = predicate.with_new_children(vec![column])?;
    mapped.downcast::<DynamicFilterPhysicalExpr>().map_err(|_| {
        datafusion::common::DataFusionError::Internal(
            "Dynamic filter remapping changed type".into(),
        )
    })
}

fn place(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: &ExecutionPlanMetricsSet,
    crossed_join: bool,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let children = predicate.children();
    let [key] = children.as_slice() else {
        return internal_err!("Early join filtering requires one key");
    };
    let Some(key) = key.downcast_ref::<Column>() else {
        return internal_err!("Early join filtering requires a column key");
    };
    if input.fetch().is_none() {
        if let Some(join) = input.downcast_ref::<DynamicFilterJoinExec>() {
            let template = join.template();
            // This wrapper is already limited to ordinary inner, single-key joins.
            // Do not skip fallible join residuals, or guess an embedded projection.
            if template.filter().is_none() && !template.contains_projection() {
                let build_columns = template.left().schema().fields().len();
                if let Some(index) = key.index().checked_sub(build_columns) {
                    if let Some(field) = template.right().schema().fields().get(index) {
                        let mapped = remap(
                            Arc::clone(&predicate),
                            Arc::new(Column::new(field.name(), index)),
                        )?;
                        if let Some(probe) = place(template.right(), mapped, metrics, true)? {
                            return Ok(Some(join.with_execution_probe(probe)?));
                        }
                    }
                }
            }
        } else if let Some(projection) = input.downcast_ref::<CometProjectionExec>() {
            let exprs = projection.projection().expr();
            // Even an unrelated computed expression may fail or be stateful.
            if exprs.iter().all(|expr| expr.expr.is::<Column>()) {
                if let Some(expr) = exprs.get(key.index()) {
                    let mapped = remap(Arc::clone(&predicate), Arc::clone(&expr.expr))?;
                    if let Some(child) = place(projection.input(), mapped, metrics, crossed_join)? {
                        return Ok(Some(projection.with_execution_input(child)?));
                    }
                }
            }
        } else if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
            if !filter.has_projection() && is_direct_column_null_checks(filter.predicate()) {
                if let Some(child) = place(
                    filter.input(),
                    Arc::clone(&predicate),
                    metrics,
                    crossed_join,
                )? {
                    return Ok(Some(filter.with_execution_input(child)?));
                }
            }
        } else if let Some(filter) = input.downcast_ref::<DynamicFilterExec>() {
            if let Some(child) =
                place(&filter.input, Arc::clone(&predicate), metrics, crossed_join)?
            {
                return Ok(Some(filter.with_execution_input(child)));
            }
        }
    }
    // A terminal consumer is useful only after crossing a join. Above other
    // boundaries the existing final consumer already performs the same work.
    Ok(crossed_join.then(|| {
        Arc::new(
            DynamicFilterExec::new(
                Arc::clone(input),
                predicate,
                metrics.clone(),
                "dynamic_filter_early",
            )
            .adaptive(),
        ) as Arc<dyn ExecutionPlan>
    }))
}
