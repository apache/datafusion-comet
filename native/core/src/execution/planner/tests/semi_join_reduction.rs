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

use super::{create_bound_reference, create_proto_datatype, PhysicalPlanner};
use datafusion_comet_operators::MembershipKeyReductionExec;
use datafusion_comet_proto::{
    spark_expression::{self as ex, expr::ExprStruct, Expr},
    spark_operator::{self as op, operator::OpStruct, JoinType, Operator},
};

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

fn semi_join(keys: &[Expr], enabled: bool) -> Operator {
    let scan = Operator {
        op_struct: Some(OpStruct::Scan(op::Scan {
            fields: vec![create_proto_datatype()],
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
    let source = semi_join(&keys, true);
    assert!(is_reduced(&source));
    assert!(!is_reduced(&semi_join(&keys, false)));

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
fn requires_the_sort_to_read_its_shuffle_leaf_directly() {
    let col = create_bound_reference(0);
    let keys = [col.clone()];
    let mut sorted = semi_join(&keys, true);
    sorted.children[1] = sort(sorted.children[1].clone(), &keys);
    assert!(!is_reduced(&sorted));

    let mut plan = semi_join(&keys, true);
    let input = &mut plan.children[1].children[0];
    input.op_struct = Some(OpStruct::ShuffleScan(op::ShuffleScan {
        fields: vec![create_proto_datatype()],
        ..Default::default()
    }));
    assert!(is_reduced(&plan));
    // The sort must read the shuffle directly.
    let input = &mut plan.children[1].children[0];
    *input = Operator {
        children: vec![std::mem::take(input)],
        op_struct: Some(OpStruct::Projection(op::Projection {
            project_list: keys.to_vec(),
        })),
        ..Default::default()
    };
    assert!(!is_reduced(&plan));
}
