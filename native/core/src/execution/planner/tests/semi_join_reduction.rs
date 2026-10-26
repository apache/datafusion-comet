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

use super::{
    create_bound_reference, create_proto_datatype, create_scan, create_sort_order, PhysicalPlanner,
};
use datafusion_comet_operators::MembershipKeyReductionExec;
use datafusion_comet_proto::spark_operator::{self as op, operator::OpStruct, JoinType, Operator};

fn reduced(planner: &PhysicalPlanner, input: Operator, sort: op::Sort) -> bool {
    let sorted = Operator {
        children: vec![input],
        op_struct: Some(OpStruct::Sort(sort)),
        ..Default::default()
    };
    let key = create_bound_reference(0);
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
    let (_, _, plan) = planner.create_plan(&join, &mut vec![], 1).unwrap();
    plan.children[1].native_plan.children()[0].is::<MembershipKeyReductionExec>()
}

#[test]
fn reduces_only_a_matching_unlimited_sort_directly_over_the_shuffle() {
    let field = create_proto_datatype();
    let scan = create_scan();
    let sort = op::Sort {
        sort_orders: vec![create_sort_order(0, field.type_id, false, true)],
        ..Default::default()
    };
    let planner = PhysicalPlanner::default();
    assert!(reduced(&planner, scan.clone(), sort.clone()));
    let busy = PhysicalPlanner::default().with_jvm_execution_memory_held(true);
    assert!(!reduced(&busy, scan.clone(), sort.clone()));
    let direct = Operator {
        op_struct: Some(OpStruct::ShuffleScan(op::ShuffleScan {
            fields: vec![field.clone()],
            ..Default::default()
        })),
        ..Default::default()
    };
    assert!(reduced(&planner, direct, sort.clone()));

    for (fetch, skip, descending, nulls_first) in [
        (Some(10), None, false, true),
        (None, Some(1), false, true),
        (None, None, true, true),
        (None, None, false, false),
    ] {
        assert!(!reduced(
            &planner,
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
    assert!(!reduced(&planner, project, sort));
}
