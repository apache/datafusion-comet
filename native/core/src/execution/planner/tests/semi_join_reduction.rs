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

use super::{collect_with_inputs, create_bound_reference, create_proto_datatype, PhysicalPlanner};
use crate::execution::{metrics::utils::to_native_metric_node, operators::InputBatch};
use arrow::array::{ArrayRef, Int32Array};
use datafusion::prelude::SessionContext;
use datafusion_comet_operators::MembershipKeyReductionExec;
use datafusion_comet_proto::{
    spark_expression::{self as ex, expr::ExprStruct, Expr},
    spark_operator::{self as op, operator::OpStruct, JoinType, Operator},
};
use std::sync::Arc;

fn sort(child: Operator, keys: &[Expr]) -> Operator {
    let sort_orders = keys
        .iter()
        .map(|key| Expr {
            expr_struct: Some(ExprStruct::SortOrder(Box::new(ex::SortOrder {
                child: Some(Box::new(key.clone())),
                direction: 0,
                null_ordering: 0,
            }))),
            ..Default::default()
        })
        .collect();
    Operator {
        children: vec![child],
        op_struct: Some(OpStruct::Sort(op::Sort {
            sort_orders,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn semi_join(columns: usize, keys: &[Expr], enabled: bool) -> Operator {
    let scan = Operator {
        op_struct: Some(OpStruct::Scan(op::Scan {
            fields: vec![create_proto_datatype(); columns],
            ..Default::default()
        })),
        ..Default::default()
    };
    let sort = sort(scan, keys);
    let Some(OpStruct::Sort(spec)) = &sort.op_struct else {
        unreachable!()
    };
    Operator {
        op_struct: Some(OpStruct::SortMergeJoin(op::SortMergeJoin {
            left_join_keys: keys.to_vec(),
            right_join_keys: keys.to_vec(),
            join_type: JoinType::LeftSemi as i32,
            sort_options: spec.sort_orders.clone(),
            right_key_reduction_enabled: enabled,
            ..Default::default()
        })),
        children: vec![sort.clone(), sort],
        ..Default::default()
    }
}

fn null_check(key: &Expr, negated: bool) -> Expr {
    let unary = Box::new(ex::UnaryExpr {
        child: Some(Box::new(key.clone())),
    });
    Expr {
        expr_struct: Some(if negated {
            ExprStruct::IsNotNull(unary)
        } else {
            ExprStruct::IsNull(unary)
        }),
        ..Default::default()
    }
}

fn null_safe_keys(key: &Expr) -> [Expr; 2] {
    let zero = Expr {
        expr_struct: Some(ExprStruct::Literal(ex::Literal {
            datatype: Some(create_proto_datatype()),
            value: Some(ex::literal::Value::IntVal(0)),
            ..Default::default()
        })),
        ..Default::default()
    };
    // Spark lowers a <=> b into a comparison of (coalesce(a, 0), isnull(a)) with the RHS tuple.
    [
        Expr {
            expr_struct: Some(ExprStruct::CaseWhen(Box::new(ex::CaseWhen {
                when: vec![null_check(key, true)],
                then: vec![key.clone()],
                else_expr: Some(Box::new(zero)),
                ..Default::default()
            }))),
            ..Default::default()
        },
        null_check(key, false),
    ]
}

fn is_reduced(join: &Operator) -> bool {
    let (_, _, plan) = PhysicalPlanner::default()
        .create_plan(join, &mut vec![], 1)
        .unwrap();
    let right = &plan.children[1].native_plan;
    right
        .children()
        .first()
        .is_some_and(|input| input.is::<MembershipKeyReductionExec>())
}

#[test]
fn requires_unfiltered_left_semi_join_and_a_matching_unlimited_sort() {
    let col = create_bound_reference(0);
    let keys = [col.clone()];
    let source = semi_join(1, &keys, true);
    assert!(is_reduced(&source));
    assert!(!is_reduced(&semi_join(1, &keys, false)));

    for (kind, condition) in [
        (JoinType::Inner, None),
        (JoinType::LeftAnti, None),
        (JoinType::LeftSemi, Some(null_check(&col, true))),
    ] {
        let mut plan = source.clone();
        let Some(OpStruct::SortMergeJoin(j)) = &mut plan.op_struct else {
            unreachable!()
        };
        j.join_type = kind as i32;
        j.condition = condition;
        assert!(!is_reduced(&plan));
    }

    for (fetch, skip, descending, nulls_last) in [
        (Some(10), None, false, false),
        (None, Some(1), false, false),
        (None, None, true, false),
        (None, None, false, true),
    ] {
        let mut plan = source.clone();
        let Some(OpStruct::Sort(s)) = &mut plan.children[1].op_struct else {
            unreachable!()
        };
        s.fetch = fetch;
        s.skip = skip;
        let Some(ExprStruct::SortOrder(order)) = &mut s.sort_orders[0].expr_struct else {
            unreachable!()
        };
        order.direction = i32::from(descending);
        order.null_ordering = i32::from(nulls_last);
        assert!(!is_reduced(&plan));
    }
    let mut mismatched = source.clone();
    let input = mismatched.children[1].children[0].clone();
    mismatched.children[1] = sort(input.clone(), &[null_check(&col, false)]);
    assert!(!is_reduced(&mismatched));
    mismatched.children[1] = input;
    assert!(!is_reduced(&mismatched));
}

#[test]
fn skips_sorted_or_unique_input_and_noncompact_or_fallible_keys() {
    let col = create_bound_reference(0);
    let keys = [col.clone()];
    let mut sorted = semi_join(1, &keys, true);
    sorted.children[1] = sort(sorted.children[1].clone(), &keys);
    assert!(!is_reduced(&sorted));

    for (mode, expected) in [
        (op::AggregateMode::Final, false),
        (op::AggregateMode::Partial, true),
    ] {
        let mut plan = semi_join(1, &keys, true);
        let input = plan.children[1].children[0].clone();
        plan.children[1].children[0] = Operator {
            children: vec![input],
            op_struct: Some(OpStruct::HashAgg(op::HashAggregate {
                grouping_exprs: keys.to_vec(),
                mode: mode as i32,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(is_reduced(&plan), expected);
    }
    assert!(!is_reduced(&semi_join(2, &keys, true)));

    // Both expressions use a compact source layout. Even under IS NULL, an arithmetic key
    // might throw, so evaluating it on fewer rows is outside the reduction contract.
    let remainder = Expr {
        expr_struct: Some(ExprStruct::Remainder(Box::new(ex::MathExpr {
            left: Some(Box::new(col.clone())),
            right: Some(Box::new(col.clone())),
            return_type: Some(create_proto_datatype()),
            ..Default::default()
        }))),
        ..Default::default()
    };
    assert!(is_reduced(&semi_join(
        1,
        &[col.clone(), null_check(&col, false)],
        true
    )));
    assert!(!is_reduced(&semi_join(
        1,
        &[col, null_check(&remainder, false)],
        true
    )));
}

#[tokio::test]
async fn composite_null_safe_keys_preserve_nulls_left_duplicates_and_metric_ownership() {
    let keys: Vec<_> = (0..2)
        .flat_map(|i| null_safe_keys(&create_bound_reference(i)))
        .collect();
    let left = [
        (None, Some(-1)),
        (None, Some(-1)),
        (Some(0), Some(-1)),
        (Some(0), None),
        (None, None),
        (Some(-2), Some(0)),
        (None, Some(0)),
    ];
    let right = [
        (None, Some(-1)),
        (Some(0), None),
        (Some(-2), Some(0)),
        (None, None),
    ];
    for rows in [0, 16_384] {
        for enabled in [false, true] {
            let mut operator = semi_join(2, &keys, enabled);
            // The RHS filter discards (NULL, NULL) before its rows reach the reducer.
            let sort_input = &mut operator.children[1].children[0];
            let scan = std::mem::take(sort_input);
            *sort_input = Operator {
                children: vec![scan],
                op_struct: Some(OpStruct::Filter(op::Filter {
                    predicate: Some(Expr {
                        expr_struct: Some(ExprStruct::Or(Box::new(ex::BinaryExpr {
                            left: Some(Box::new(null_check(&create_bound_reference(0), true))),
                            right: Some(Box::new(null_check(&create_bound_reference(1), true))),
                        }))),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            let session = Arc::new(SessionContext::new());
            let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
            let (mut scans, _, plan) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
            let batch = |tuples: &[(Option<i32>, Option<i32>)], n: usize| {
                let tuples = tuples.iter().cycle().take(n);
                let a = Int32Array::from_iter(tuples.clone().map(|(a, _)| *a));
                let b = Int32Array::from_iter(tuples.map(|(_, b)| *b));
                InputBatch::Batch(vec![Arc::new(a) as ArrayRef, Arc::new(b)], n)
            };
            let mut inputs = [
                vec![batch(&left, left.len()), InputBatch::EOF].into_iter(),
                vec![batch(&right, rows), InputBatch::EOF].into_iter(),
            ];
            let stream = plan.native_plan.execute(0, session.task_ctx()).unwrap();
            let mut output = Vec::new();
            for batch in collect_with_inputs(stream, &mut scans, &mut inputs).await {
                let a = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let b = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                output.extend(a.iter().zip(b));
            }
            output.sort_unstable();
            let mut expected = if rows > 0 {
                vec![left[0], left[1], left[3], left[5]]
            } else {
                vec![]
            };
            expected.sort_unstable();
            assert_eq!(output, expected);

            if enabled {
                let metrics = to_native_metric_node(&plan).unwrap();
                let reduced = metrics.metrics["right_key_reduction_output_rows"];
                let after_filter = (rows * 3 / 4) as i64;
                assert_eq!(
                    metrics.metrics["right_key_reduction_input_rows"],
                    after_filter
                );
                if rows == 0 {
                    assert_eq!(reduced, 0);
                } else {
                    assert!(reduced > 0 && reduced < after_filter);
                }
                assert_eq!(metrics.metrics["output_rows"], output.len() as i64);
                let sort_metrics = &metrics.children[1].metrics;
                assert_eq!(sort_metrics["output_rows"], reduced);
                assert!(!sort_metrics.contains_key("right_key_reduction_output_rows"));
            }
        }
    }
}
