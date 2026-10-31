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

use std::sync::Arc;

use super::{
    create_bound_reference, create_proto_datatype, create_scan, create_sort_order, PhysicalPlanner,
};
use datafusion::{
    physical_expr::expressions::Column,
    physical_plan::{
        joins::SortMergeJoinExec, projection::ProjectionExec, sorts::sort::SortExec, ExecutionPlan,
    },
};
use datafusion_comet_operators::MembershipKeyReductionExec;
use datafusion_comet_proto::spark_expression::{self as expr, expr::ExprStruct, Expr};
use datafusion_comet_proto::spark_operator::{self as op, operator::OpStruct, JoinType, Operator};

fn plan_join(input: Operator, sort: op::Sort, key: Expr) -> Arc<dyn ExecutionPlan> {
    let sorted = Operator {
        children: vec![input],
        op_struct: Some(OpStruct::Sort(sort)),
        ..Default::default()
    };
    let join = Operator {
        children: vec![sorted.clone(), sorted],
        op_struct: Some(OpStruct::SortMergeJoin(op::SortMergeJoin {
            left_join_keys: vec![key.clone()],
            right_join_keys: vec![key],
            join_type: JoinType::LeftSemi as i32,
            sort_options: vec![create_sort_order(
                0,
                create_proto_datatype().type_id,
                false,
                true,
            )],
            right_key_reduction_enabled: true,
            ..Default::default()
        })),
        ..Default::default()
    };
    let (_, _, plan) = PhysicalPlanner::default()
        .create_plan(&join, &mut vec![], 1)
        .unwrap();
    Arc::clone(&plan.native_plan)
}

#[test]
fn reduces_only_a_matching_unlimited_sort_directly_over_the_shuffle() {
    let reduced = |input, sort| {
        plan_join(input, sort, create_bound_reference(0)).children()[1].children()[0]
            .is::<MembershipKeyReductionExec>()
    };
    let field = create_proto_datatype();
    let scan = create_scan();
    let sort = op::Sort {
        sort_orders: vec![create_sort_order(0, field.type_id, false, true)],
        ..Default::default()
    };
    assert!(reduced(scan.clone(), sort.clone()));
    let direct = Operator {
        op_struct: Some(OpStruct::ShuffleScan(op::ShuffleScan {
            fields: vec![field.clone()],
            ..Default::default()
        })),
        ..Default::default()
    };
    assert!(reduced(direct, sort.clone()));

    for (fetch, skip, descending, nulls_first) in [
        (Some(10), None, false, true),
        (None, Some(1), false, true),
        (None, None, true, true),
        (None, None, false, false),
    ] {
        assert!(!reduced(
            scan.clone(),
            op::Sort {
                fetch,
                skip,
                sort_orders: vec![create_sort_order(0, field.type_id, descending, nulls_first)],
                ..Default::default()
            }
        ));
    }
    let project = Operator {
        children: vec![scan],
        op_struct: Some(OpStruct::Projection(op::Projection {
            project_list: vec![create_bound_reference(0)],
        })),
        ..Default::default()
    };
    assert!(!reduced(project, sort));
}

#[test]
fn projects_an_integer_coalesce_before_reducing_its_key() {
    let wrap = |kind| Expr {
        expr_struct: Some(kind),
        ..Default::default()
    };
    let plan_key = |key: Expr| {
        let sort = op::Sort {
            sort_orders: vec![wrap(ExprStruct::SortOrder(Box::new(expr::SortOrder {
                child: Some(Box::new(key.clone())),
                ..Default::default()
            })))],
            ..Default::default()
        };
        plan_join(create_scan(), sort, key)
    };
    let column = create_bound_reference(0);
    let has_value = wrap(ExprStruct::IsNotNull(Box::new(expr::UnaryExpr {
        child: Some(Box::new(column.clone())),
    })));
    // Spark lowers coalesce(k, 0) to CASE WHEN k IS NOT NULL THEN k ELSE 0 END.
    let key = wrap(ExprStruct::CaseWhen(Box::new(expr::CaseWhen {
        when: vec![has_value.clone()],
        then: vec![column],
        else_expr: Some(Box::new(wrap(ExprStruct::Literal(expr::Literal {
            datatype: Some(create_proto_datatype()),
            value: Some(expr::literal::Value::IntVal(0)),
            ..Default::default()
        })))),
        ..Default::default()
    })));
    let plan = plan_key(key);
    let join = plan.downcast_ref::<SortMergeJoinExec>().unwrap();
    let sort = join.right().downcast_ref::<SortExec>().unwrap();
    let reducer = sort
        .input()
        .downcast_ref::<MembershipKeyReductionExec>()
        .unwrap();
    let projection = reducer.children()[0]
        .downcast_ref::<ProjectionExec>()
        .unwrap();
    let key = &join.on()[0].1;
    assert_eq!(
        key.downcast_ref::<Column>().unwrap(),
        &Column::new("membership_key", 0)
    );
    assert!(sort.expr().first().expr.eq(key));
    assert_eq!(projection.expr()[0].alias, "membership_key");

    // A computed Boolean key still uses the original sort input.
    let boolean_plan = plan_key(has_value);
    let join = boolean_plan.downcast_ref::<SortMergeJoinExec>().unwrap();
    let sort = join.right().downcast_ref::<SortExec>().unwrap();
    assert!(sort.input().is::<super::ScanExec>());
}
