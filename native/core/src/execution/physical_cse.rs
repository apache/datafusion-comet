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

//! Physical common subexpression elimination (CSE) optimizer rule.
//!
//! Identifies repeated subexpressions within `ProjectionExec` and
//! `AggregateExec` nodes and rewrites the plan to compute them once via an
//! intermediate projection.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::ExecutionPlan;

/// A wrapper around `Arc<dyn PhysicalExpr>` that implements `Eq` and `Hash`
/// by delegating to the trait-object implementations on `dyn PhysicalExpr`.
struct ExprKey(Arc<dyn datafusion::physical_plan::PhysicalExpr>);

impl PartialEq for ExprKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for ExprKey {}

impl Hash for ExprKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state);
    }
}

/// Physical optimizer rule that eliminates common subexpressions within
/// `ProjectionExec` and `AggregateExec` nodes.
#[derive(Debug)]
pub struct PhysicalCommonSubexprEliminate;

impl PhysicalCommonSubexprEliminate {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for PhysicalCommonSubexprEliminate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if node.as_any().downcast_ref::<ProjectionExec>().is_some() {
                try_optimize_projection(node)
            } else if node.as_any().downcast_ref::<AggregateExec>().is_some() {
                try_optimize_aggregate(node)
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "physical_common_subexpr_eliminate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns `true` if the expression is trivial (a `Column` or `Literal`)
/// and therefore not worth extracting as a common subexpression.
fn is_trivial(expr: &Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> bool {
    expr.as_any().downcast_ref::<Column>().is_some()
        || expr.as_any().downcast_ref::<Literal>().is_some()
}

/// Recursively collect all sub-expressions from `expr` and increment their
/// occurrence count in `counts`.
fn collect_subexprs(
    expr: &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    counts: &mut HashMap<ExprKey, usize>,
) {
    if is_trivial(expr) || is_volatile(expr) {
        return;
    }
    let key = ExprKey(Arc::clone(expr));
    *counts.entry(key).or_insert(0) += 1;
    for child in expr.children() {
        collect_subexprs(child, counts);
    }
}

/// Identify sub-expressions that appear 2+ times across the given
/// expression list. Returns a deduplicated vec.
fn find_common_subexprs(
    exprs: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
) -> Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    let mut counts: HashMap<ExprKey, usize> = HashMap::new();
    for expr in exprs {
        collect_subexprs(expr, &mut counts);
    }
    // Collect expressions with count >= 2, preserving insertion order is not
    // required since we assign deterministic names based on index.
    counts
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .map(|(key, _)| key.0)
        .collect()
}

/// Replace occurrences of any common subexpression in `expr` with a `Column`
/// reference into the intermediate projection's schema.
fn rewrite_expr(
    expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    cse_map: &HashMap<ExprKey, (String, usize)>,
) -> Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    expr.transform_down(|node| {
        if is_trivial(&node) {
            return Ok(Transformed::no(node));
        }
        let lookup = ExprKey(Arc::clone(&node));
        if let Some((name, index)) = cse_map.get(&lookup) {
            // Replace with a column reference and skip recursing into children
            let col = Arc::new(Column::new(name, *index))
                as Arc<dyn datafusion::physical_plan::PhysicalExpr>;
            Ok(Transformed::new(col, true, TreeNodeRecursion::Jump))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .data()
}

/// Attempt to optimize a single `ProjectionExec` by extracting common
/// subexpressions into an intermediate projection.
fn try_optimize_projection(
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let projection = node.as_any().downcast_ref::<ProjectionExec>().unwrap();
    let proj_exprs = projection.expr();

    let raw_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> =
        proj_exprs.iter().map(|pe| Arc::clone(&pe.expr)).collect();
    let common = find_common_subexprs(&raw_exprs);

    if common.is_empty() {
        return Ok(Transformed::no(node));
    }

    let input = projection.input();
    let input_schema = input.schema();
    let num_input_cols = input_schema.fields().len();

    // Build the intermediate projection: pass through all input columns, then
    // append one column per common subexpression.
    let mut intermediate_exprs: Vec<ProjectionExpr> = Vec::new();
    for (i, field) in input_schema.fields().iter().enumerate() {
        intermediate_exprs.push(ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), i)),
            alias: field.name().clone(),
        });
    }

    // Map from common subexpression -> (cse_name, column_index_in_intermediate)
    let mut cse_map: HashMap<ExprKey, (String, usize)> = HashMap::new();
    for (idx, cse_expr) in common.iter().enumerate() {
        let cse_name = format!("__cse_{idx}");
        let col_index = num_input_cols + idx;
        intermediate_exprs.push(ProjectionExpr {
            expr: Arc::clone(cse_expr),
            alias: cse_name.clone(),
        });
        cse_map.insert(ExprKey(Arc::clone(cse_expr)), (cse_name, col_index));
    }

    let intermediate = Arc::new(ProjectionExec::try_new(
        intermediate_exprs,
        Arc::clone(input),
    )?) as Arc<dyn ExecutionPlan>;

    // Rewrite the top projection expressions to reference the intermediate.
    let mut new_proj_exprs: Vec<ProjectionExpr> = Vec::new();
    for proj_expr in proj_exprs {
        let rewritten = rewrite_expr(Arc::clone(&proj_expr.expr), &cse_map)?;
        new_proj_exprs.push(ProjectionExpr {
            expr: rewritten,
            alias: proj_expr.alias.clone(),
        });
    }

    let new_projection =
        Arc::new(ProjectionExec::try_new(new_proj_exprs, intermediate)?) as Arc<dyn ExecutionPlan>;

    Ok(Transformed::yes(new_projection))
}

/// Attempt to optimize a single `AggregateExec` by extracting common
/// subexpressions from aggregate function arguments into an intermediate
/// projection.
fn try_optimize_aggregate(
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let agg_exec = node.as_any().downcast_ref::<AggregateExec>().unwrap();

    // Only optimize first-stage aggregates where original column expressions
    // are present. Final/FinalPartitioned reference partial outputs.
    if !agg_exec.mode().is_first_stage() {
        return Ok(Transformed::no(node));
    }

    // Collect all sub-expressions from aggregate function arguments.
    let aggr_exprs = agg_exec.aggr_expr();
    let all_args: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> = aggr_exprs
        .iter()
        .flat_map(|agg_fn| agg_fn.expressions())
        .collect();

    let common = find_common_subexprs(&all_args);
    if common.is_empty() {
        return Ok(Transformed::no(node));
    }

    let input = agg_exec.input();
    let input_schema = input.schema();
    let num_input_cols = input_schema.fields().len();

    // Build an intermediate projection: pass through all input columns, then
    // append one column per common subexpression.
    let mut intermediate_exprs: Vec<ProjectionExpr> = Vec::new();
    for (i, field) in input_schema.fields().iter().enumerate() {
        intermediate_exprs.push(ProjectionExpr {
            expr: Arc::new(Column::new(field.name(), i)),
            alias: field.name().clone(),
        });
    }

    // Map from common subexpression -> (cse_name, column_index_in_intermediate)
    let mut cse_map: HashMap<ExprKey, (String, usize)> = HashMap::new();
    for (idx, cse_expr) in common.iter().enumerate() {
        let cse_name = format!("__cse_{idx}");
        let col_index = num_input_cols + idx;
        intermediate_exprs.push(ProjectionExpr {
            expr: Arc::clone(cse_expr),
            alias: cse_name.clone(),
        });
        cse_map.insert(ExprKey(Arc::clone(cse_expr)), (cse_name, col_index));
    }

    let intermediate = Arc::new(ProjectionExec::try_new(
        intermediate_exprs,
        Arc::clone(input),
    )?) as Arc<dyn ExecutionPlan>;
    let intermediate_schema = intermediate.schema();

    // Rewrite each aggregate function's arguments to reference CSE columns.
    let mut new_aggr_exprs: Vec<Arc<AggregateFunctionExpr>> = Vec::new();
    for agg_fn in aggr_exprs {
        let old_args = agg_fn.expressions();
        let mut new_args = Vec::with_capacity(old_args.len());
        for arg in &old_args {
            new_args.push(rewrite_expr(Arc::clone(arg), &cse_map)?);
        }
        let order_by_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> = agg_fn
            .order_bys()
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect();
        let new_agg_fn = agg_fn
            .with_new_expressions(new_args, order_by_exprs)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(format!(
                    "Failed to rewrite aggregate expression: {}",
                    agg_fn.name()
                ))
            })?;
        new_aggr_exprs.push(Arc::new(new_agg_fn));
    }

    // Rewrite filter expressions if they reference common subexpressions.
    let new_filters: Vec<Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>> = agg_exec
        .filter_expr()
        .iter()
        .map(|filter_opt| {
            filter_opt
                .as_ref()
                .map(|f| rewrite_expr(Arc::clone(f), &cse_map))
                .transpose()
        })
        .collect::<Result<_>>()?;

    // Rewrite group-by expressions to reference CSE columns.
    let old_group_by = agg_exec.group_expr();
    let new_group_exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> =
        old_group_by
            .expr()
            .iter()
            .map(|(expr, alias)| {
                Ok((rewrite_expr(Arc::clone(expr), &cse_map)?, alias.clone()))
            })
            .collect::<Result<_>>()?;
    let new_null_exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> =
        old_group_by
            .null_expr()
            .iter()
            .map(|(expr, alias)| (Arc::clone(expr), alias.clone()))
            .collect();
    let new_group_by =
        PhysicalGroupBy::new(new_group_exprs, new_null_exprs, old_group_by.groups().to_vec());

    let new_agg = AggregateExec::try_new(
        *agg_exec.mode(),
        new_group_by,
        new_aggr_exprs,
        new_filters,
        intermediate,
        intermediate_schema,
    )?;

    Ok(Transformed::yes(Arc::new(new_agg) as Arc<dyn ExecutionPlan>))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{binary, col};
    use datafusion::physical_plan::aggregates::AggregateMode;
    use datafusion::physical_plan::empty::EmptyExec;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    #[test]
    fn test_cse_extracts_common_subexpr() -> Result<()> {
        let schema = test_schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let a = col("a", &schema)?;
        let b = col("b", &schema)?;

        // (a + b) * 2, (a + b) * 3 — both share (a + b)
        let a_plus_b_1 = binary(Arc::clone(&a), Operator::Plus, Arc::clone(&b), &schema)?;
        let a_plus_b_2 = binary(Arc::clone(&a), Operator::Plus, Arc::clone(&b), &schema)?;

        let two = Arc::new(Literal::new(datafusion::common::ScalarValue::Int32(Some(
            2,
        ))));
        let three = Arc::new(Literal::new(datafusion::common::ScalarValue::Int32(Some(
            3,
        ))));

        let expr_x = binary(a_plus_b_1, Operator::Multiply, two, &schema)?;
        let expr_y = binary(a_plus_b_2, Operator::Multiply, three, &schema)?;

        let projection = ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: expr_x,
                    alias: "x".to_string(),
                },
                ProjectionExpr {
                    expr: expr_y,
                    alias: "y".to_string(),
                },
            ],
            input,
        )?;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection);
        let config = ConfigOptions::new();
        let rule = PhysicalCommonSubexprEliminate::new();
        let optimized = rule.optimize(plan, &config)?;

        // The optimized plan should be a ProjectionExec wrapping another ProjectionExec
        let top = optimized
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("top should be ProjectionExec");
        let intermediate = top
            .input()
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("intermediate should be ProjectionExec");

        // Intermediate should have input columns + 1 CSE column
        assert_eq!(intermediate.expr().len(), 3); // a, b, __cse_0

        // The CSE column should be named __cse_0
        let cse_alias = &intermediate.expr()[2].alias;
        assert_eq!(cse_alias, "__cse_0");

        // Top projection should still produce 2 output columns
        assert_eq!(top.expr().len(), 2);
        assert_eq!(top.expr()[0].alias, "x");
        assert_eq!(top.expr()[1].alias, "y");

        Ok(())
    }

    #[test]
    fn test_no_cse_when_no_common_subexpr() -> Result<()> {
        let schema = test_schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let a = col("a", &schema)?;
        let b = col("b", &schema)?;

        let projection = ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: a,
                    alias: "a".to_string(),
                },
                ProjectionExpr {
                    expr: b,
                    alias: "b".to_string(),
                },
            ],
            input,
        )?;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection);
        let config = ConfigOptions::new();
        let rule = PhysicalCommonSubexprEliminate::new();
        let optimized = rule.optimize(Arc::clone(&plan), &config)?;

        // No intermediate projection should be inserted
        let top = optimized
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("should be ProjectionExec");
        // Input should be EmptyExec, not another ProjectionExec
        assert!(top
            .input()
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .is_none());

        Ok(())
    }

    #[test]
    fn test_aggregate_cse_extracts_common_subexpr() -> Result<()> {
        // Schema: a INT64, b INT64, c INT64
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let c = col("c", &schema)?;

        // Common subexpression: a + b
        let a_plus_b_1 = binary(Arc::clone(&a), Operator::Plus, Arc::clone(&b), &schema)?;
        let a_plus_b_2 = binary(Arc::clone(&a), Operator::Plus, Arc::clone(&b), &schema)?;

        // sum(a + b) and sum((a + b) * c) — both share (a + b)
        let agg1 = AggregateExprBuilder::new(sum_udaf(), vec![a_plus_b_1])
            .schema(Arc::clone(&schema))
            .alias("sum1")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let expr2 = binary(a_plus_b_2, Operator::Multiply, Arc::clone(&c), &schema)?;
        let agg2 = AggregateExprBuilder::new(sum_udaf(), vec![expr2])
            .schema(Arc::clone(&schema))
            .alias("sum2")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let group_by = PhysicalGroupBy::new_single(vec![]);
        let aggregate = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![Arc::new(agg1), Arc::new(agg2)],
            vec![None, None],
            input,
            Arc::clone(&schema),
        )?;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(aggregate);
        let config = ConfigOptions::new();
        let rule = PhysicalCommonSubexprEliminate::new();
        let optimized = rule.optimize(plan, &config)?;

        // The optimized plan should be an AggregateExec wrapping a ProjectionExec
        let top_agg = optimized
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("top should be AggregateExec");
        let intermediate = top_agg
            .input()
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("intermediate should be ProjectionExec");

        // Intermediate should have input columns (a, b, c) + 1 CSE column
        assert_eq!(intermediate.expr().len(), 4); // a, b, c, __cse_0

        // The CSE column should be named __cse_0
        let cse_alias = &intermediate.expr()[3].alias;
        assert_eq!(cse_alias, "__cse_0");

        // Aggregate should still produce 2 aggregate expressions
        assert_eq!(top_agg.aggr_expr().len(), 2);

        Ok(())
    }

    #[test]
    fn test_aggregate_cse_skips_final_mode() -> Result<()> {
        // Final-mode aggregates should not be optimized
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let a = col("a", &schema)?;

        let agg1 = AggregateExprBuilder::new(sum_udaf(), vec![Arc::clone(&a)])
            .schema(Arc::clone(&schema))
            .alias("sum1")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let agg2 = AggregateExprBuilder::new(sum_udaf(), vec![Arc::clone(&a)])
            .schema(Arc::clone(&schema))
            .alias("sum2")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let group_by = PhysicalGroupBy::new_single(vec![]);
        let aggregate = AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            vec![Arc::new(agg1), Arc::new(agg2)],
            vec![None, None],
            input,
            Arc::clone(&schema),
        )?;

        let plan: Arc<dyn ExecutionPlan> = Arc::new(aggregate);
        let config = ConfigOptions::new();
        let rule = PhysicalCommonSubexprEliminate::new();
        let optimized = rule.optimize(plan, &config)?;

        // Should still be an AggregateExec with no intermediate ProjectionExec
        let top_agg = optimized
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("should be AggregateExec");
        assert!(
            top_agg
                .input()
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .is_none(),
            "Final-mode aggregate should not have intermediate projection"
        );

        Ok(())
    }
}
