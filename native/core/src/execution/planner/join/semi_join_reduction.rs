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

//! Reduce repeated right-side rows before sorting an unfiltered left-semi join.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::{
    arrow::compute::SortOptions,
    common::JoinType,
    functions::core::coalesce::CoalesceFunc,
    physical_expr::{
        expressions::{CaseExpr, Column, IsNotNullExpr, IsNullExpr, Literal},
        LexOrdering, PhysicalExpr, ScalarFunctionExpr,
    },
    physical_plan::{
        aggregates::{AggregateExec, AggregateMode},
        limit::{GlobalLimitExec, LocalLimitExec},
        sorts::sort::SortExec,
        ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
    },
};
use datafusion_comet_operators::MembershipKeyReductionExec;
use datafusion_comet_spark_expr::{CaseWhenExpr, Cast, IfExpr};

use super::JoinParameters;
use crate::execution::operators::ExecutionError;

/// Removing equal source rows must not suppress errors or change evaluation of stateful keys.
/// Spark encodes null-safe equality as (coalesce(key, default), isnull(key)), and Comet
/// serializes coalesce as CASE WHEN. Check every child, including its casts.
fn supported_key(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    let Ok(data_type) = expr.data_type(schema) else {
        return false;
    };
    if !data_type.is_signed_integer() && data_type != DataType::Boolean {
        return false;
    }
    let children = expr.children();
    let supported = expr.is::<Column>()
        || expr.is::<Literal>()
        || expr.is::<IsNullExpr>()
        || expr.is::<IsNotNullExpr>()
        || expr.is::<CaseWhenExpr>()
        || expr.is::<CaseExpr>()
        || expr.is::<IfExpr>()
        || (expr.is::<Cast>()
            && matches!(children.as_slice(), [child] if child.data_type(schema).is_ok_and(|t| t == data_type)))
        || expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|f| f.fun().inner().is::<CoalesceFunc>());
    supported
        && children
            .into_iter()
            .all(|child| supported_key(child, schema))
}

/// A final aggregate is already unique when every grouping column occurs in the membership
/// tuple. Partial aggregates can emit the same group more than once under memory pressure.
fn already_unique(input: &Arc<dyn ExecutionPlan>, ordering: &LexOrdering) -> bool {
    let Some(aggregate) = input.downcast_ref::<AggregateExec>() else {
        return false;
    };
    matches!(
        aggregate.mode(),
        AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned
    ) && !aggregate.group_expr().has_grouping_set()
        && (0..aggregate.group_expr().expr().len()).all(|index| {
            ordering.iter().any(|key| {
                key.expr
                    .downcast_ref::<Column>()
                    .is_some_and(|col| col.index() == index)
            })
        })
}

/// Group complete integer source rows. A null-safe tuple can use fewer source columns
/// than comparison components. For an equally wide tuple, only accept the source columns
/// themselves in order. Keep the sort and join expressions, including their null ordering.
fn compact_integer_source(schema: &Schema, ordering: &LexOrdering) -> bool {
    let fields = schema.fields();
    if fields.is_empty()
        || fields.len() > ordering.len()
        || !fields.iter().all(|f| f.data_type().is_signed_integer())
    {
        return false;
    }
    if fields.len() == ordering.len() {
        return ordering.iter().enumerate().all(|(i, key)| {
            key.expr
                .downcast_ref::<Column>()
                .is_some_and(|col| col.index() == i)
        });
    }
    let source_width: usize = fields
        .iter()
        .filter_map(|f| f.data_type().primitive_width())
        .sum();
    // Budget one byte for each Boolean discriminator.
    let key_width: Option<usize> = ordering
        .iter()
        .map(|key| {
            key.expr
                .data_type(schema)
                .ok()
                .map(|t| t.primitive_width().unwrap_or(1))
        })
        .sum();
    key_width.is_some_and(|width| source_width <= width)
}

pub(super) fn reduce_before_sort(
    params: &mut JoinParameters,
    sort_options: &[SortOptions],
) -> Result<Option<Arc<dyn ExecutionPlan>>, ExecutionError> {
    if params.join_type != JoinType::LeftSemi
        || params.join_filter.is_some()
        || params.join_on.is_empty()
    {
        return Ok(None);
    }
    let Some(sort) = params.right.native_plan.downcast_ref::<SortExec>() else {
        return Ok(None);
    };
    let input = sort.input();
    let ordering = sort.expr();
    let schema = input.schema();
    if sort.fetch().is_some()
        || input.boundedness().is_unbounded()
        || input.is::<LocalLimitExec>()
        || input.is::<GlobalLimitExec>()
        || input.is::<MembershipKeyReductionExec>()
        || ordering.len() != params.join_on.len()
        || ordering.len() != sort_options.len()
    {
        return Ok(None);
    }

    let matching_keys = ordering.iter().zip(&params.join_on).zip(sort_options).all(
        |((order, (_, key)), options)| {
            order.options == *options && order.expr.eq(key) && supported_key(key, &schema)
        },
    );
    if !matching_keys
        || !compact_integer_source(&schema, ordering)
        || already_unique(input, ordering)
    {
        return Ok(None);
    }
    // Reordering an already sorted input would turn a streaming sort into a blocking one.
    if input
        .equivalence_properties()
        .ordering_satisfy(ordering.clone())?
    {
        return Ok(None);
    }

    let reducer: Arc<dyn ExecutionPlan> =
        Arc::new(MembershipKeyReductionExec::new(Arc::clone(input)));
    let sort = Arc::clone(&params.right.native_plan).replace_children(
        vec![Arc::clone(&reducer)],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    Arc::make_mut(&mut params.right).native_plan = sort;
    // The Spark sort collects from its replacement; the join owns the reducer's named metrics.
    Ok(Some(reducer))
}
