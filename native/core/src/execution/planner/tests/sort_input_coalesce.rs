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
use datafusion_comet_operators::{MembershipKeyReductionExec, SortInputCoalesceExec};
use datafusion_comet_proto::spark_operator::{self as op, operator::OpStruct, JoinType, Operator};

fn sort(coalesce_input: bool, fetch: Option<i32>) -> Operator {
    sort_with_skip(coalesce_input, fetch, None)
}

fn sort_with_skip(coalesce_input: bool, fetch: Option<i32>, skip: Option<i32>) -> Operator {
    Operator {
        children: vec![create_scan()],
        op_struct: Some(OpStruct::Sort(op::Sort {
            sort_orders: vec![create_sort_order(
                0,
                create_proto_datatype().type_id,
                false,
                true,
            )],
            fetch,
            skip,
            coalesce_input,
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[test]
fn wraps_the_input_of_a_full_sort_only() {
    let plan = |op: Operator| {
        PhysicalPlanner::default()
            .create_plan(&op, &mut vec![], 1)
            .unwrap()
            .2
    };

    let full = plan(sort(true, None));
    let input = &full.native_plan.children()[0];
    assert!(input.is::<SortInputCoalesceExec>());
    // The session's batch size and the sort's in-place threshold reach the coalescer.
    let defaults = datafusion::prelude::SessionConfig::new();
    assert_eq!(
        datafusion::physical_plan::displayable(input.as_ref())
            .one_line()
            .to_string()
            .trim(),
        format!(
            "CometSortInputCoalesceExec: target_rows={}, gate_bytes={}, max_bytes={}",
            defaults.batch_size(),
            defaults.options().execution.sort_in_place_threshold_bytes,
            defaults.options().execution.sort_in_place_threshold_bytes / 2
        )
    );
    // Its metrics are reported under the sort.
    assert_eq!(full.additional_native_plans.len(), 1);
    assert!(full.additional_native_plans[0].is::<SortInputCoalesceExec>());

    for op in [sort(false, None), sort(true, Some(10))] {
        let planned = plan(op);
        assert!(!planned.native_plan.children()[0].is::<SortInputCoalesceExec>());
        assert!(planned.additional_native_plans.is_empty());
    }
}

#[test]
fn semi_join_key_reduction_goes_below_the_coalescer() {
    let key = create_bound_reference(0);
    let sorted = sort(true, None);
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
    let right = &plan.children[1];
    // The reducer forwards blocks it cannot reduce unchanged, so they still get combined.
    let coalescer = &right.native_plan.children()[0];
    assert!(coalescer.is::<SortInputCoalesceExec>());
    assert!(coalescer.children()[0].is::<MembershipKeyReductionExec>());
    assert_eq!(right.additional_native_plans.len(), 1);
    assert!(right.additional_native_plans[0].is::<SortInputCoalesceExec>());
    // The left sort keeps its coalescer.
    assert!(plan.children[0].native_plan.children()[0].is::<SortInputCoalesceExec>());
}

#[test]
fn a_sort_with_only_a_skip_still_gets_its_input_combined() {
    let (_, _, plan) = PhysicalPlanner::default()
        .create_plan(&sort_with_skip(true, None, Some(1)), &mut vec![], 1)
        .unwrap();
    // The skip wraps the sort in a limit; the sort itself still has no fetch.
    let sort = &plan.native_plan.children()[0];
    assert!(sort.children()[0].is::<SortInputCoalesceExec>());
    assert_eq!(plan.additional_native_plans.len(), 1);
}

#[test]
fn a_sort_over_input_already_in_its_order_is_not_wrapped() {
    // The inner sort's output is already in the outer sort's order, so the outer sort passes it
    // through and combining it would only copy.
    let outer = Operator {
        children: vec![sort(true, None)],
        op_struct: Some(OpStruct::Sort(op::Sort {
            sort_orders: vec![create_sort_order(
                0,
                create_proto_datatype().type_id,
                false,
                true,
            )],
            coalesce_input: true,
            ..Default::default()
        })),
        ..Default::default()
    };
    let (_, _, plan) = PhysicalPlanner::default()
        .create_plan(&outer, &mut vec![], 1)
        .unwrap();
    assert!(!plan.native_plan.children()[0].is::<SortInputCoalesceExec>());
    assert!(plan.additional_native_plans.is_empty());
    // The inner sort, over an unordered scan, still is.
    assert!(plan.children[0].native_plan.children()[0].is::<SortInputCoalesceExec>());
}
