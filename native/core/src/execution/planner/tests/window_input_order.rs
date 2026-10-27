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

//! A native window sorts its input unless every PARTITION BY key is ordered.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression::{
    self, data_type::DataTypeId, expr::ExprStruct, Expr,
};
use datafusion_comet_proto::spark_operator::{
    self, lower_window_frame_bound::LowerFrameBoundStruct, operator::OpStruct,
    upper_window_frame_bound::UpperFrameBoundStruct, CurrentRow, LowerWindowFrameBound, Operator,
    UnboundedPreceding, UpperWindowFrameBound, WindowFrame, WindowFrameType, WindowSpecDefinition,
};

use super::collect_with_inputs;
use crate::execution::metrics::utils::to_native_metric_node;
use crate::execution::operators::InputBatch;
use crate::execution::planner::PhysicalPlanner;

/// `row_number` runs in `BoundedWindowAggExec` and `percent_rank` in `WindowAggExec`.
const FUNCTIONS: [(&str, &str); 2] = [
    ("row_number", "BoundedWindowAggExec"),
    ("percent_rank", "WindowAggExec"),
];

fn data_type(type_id: DataTypeId) -> spark_expression::DataType {
    spark_expression::DataType {
        type_id: type_id as i32,
        type_info: None,
    }
}

fn int32() -> spark_expression::DataType {
    data_type(DataTypeId::Int32)
}

fn typed_column(index: i32, type_id: DataTypeId) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::Bound(spark_expression::BoundReference {
            index,
            datatype: Some(data_type(type_id)),
        })),
        ..Default::default()
    }
}

fn column(index: i32) -> Expr {
    typed_column(index, DataTypeId::Int32)
}

/// Spark's default null ordering: ASC NULLS FIRST, DESC NULLS LAST.
fn sort_order_of(child: Expr, descending: bool) -> Expr {
    Expr {
        expr_struct: Some(ExprStruct::SortOrder(Box::new(
            spark_expression::SortOrder {
                child: Some(Box::new(child)),
                direction: i32::from(descending),
                null_ordering: i32::from(descending),
            },
        ))),
        ..Default::default()
    }
}

fn sort_order(index: i32, descending: bool) -> Expr {
    sort_order_of(column(index), descending)
}

/// Columns from the JVM, which declares no order.
fn scan_of(fields: Vec<spark_expression::DataType>) -> Operator {
    Operator {
        op_struct: Some(OpStruct::Scan(spark_operator::Scan {
            fields,
            source: String::new(),
        })),
        ..Default::default()
    }
}

/// Three Int32 columns, (a, b, v).
fn scan() -> Operator {
    scan_of(vec![int32(); 3])
}

fn sort_by(child: Operator, sort_orders: Vec<Expr>) -> Operator {
    Operator {
        children: vec![child],
        op_struct: Some(OpStruct::Sort(spark_operator::Sort {
            sort_orders,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn sort(child: Operator, order: &[(i32, bool)]) -> Operator {
    let orders = order.iter().map(|&(i, desc)| sort_order(i, desc)).collect();
    sort_by(child, orders)
}

fn window(child: Operator, function: &str, partition_by: &[i32], order_by: &[i32]) -> Operator {
    let partition_by = partition_by.iter().map(|&i| column(i)).collect();
    let order_by = order_by.iter().map(|&i| sort_order(i, false)).collect();
    window_by(child, function, partition_by, order_by, None)
}

/// A window with one function. A `result_type` that differs from the native one makes the
/// planner cast the result in a projection above the window.
fn window_by(
    child: Operator,
    function: &str,
    partition_by_list: Vec<Expr>,
    order_by_list: Vec<Expr>,
    result_type: Option<spark_expression::DataType>,
) -> Operator {
    let expr = spark_operator::WindowExpr {
        built_in_window_function: Some(Expr {
            expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                func: function.to_string(),
                ..Default::default()
            })),
            ..Default::default()
        }),
        spec: Some(WindowSpecDefinition {
            frame_specification: Some(WindowFrame {
                frame_type: WindowFrameType::Rows as i32,
                lower_bound: Some(LowerWindowFrameBound {
                    lower_frame_bound_struct: Some(LowerFrameBoundStruct::UnboundedPreceding(
                        UnboundedPreceding {},
                    )),
                }),
                upper_bound: Some(UpperWindowFrameBound {
                    upper_frame_bound_struct: Some(UpperFrameBoundStruct::CurrentRow(
                        CurrentRow {},
                    )),
                }),
            }),
            ..Default::default()
        }),
        result_type,
        ..Default::default()
    };
    Operator {
        children: vec![child],
        op_struct: Some(OpStruct::Window(Box::new(spark_operator::Window {
            window_expr: vec![expr],
            partition_by_list,
            order_by_list,
            ..Default::default()
        }))),
        ..Default::default()
    }
}

fn find_window(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if matches!(plan.name(), "BoundedWindowAggExec" | "WindowAggExec") {
        return Arc::clone(plan);
    }
    find_window(plan.children().first().expect("no window in the plan"))
}

fn count_sorts(plan: &Arc<dyn ExecutionPlan>) -> usize {
    usize::from(plan.downcast_ref::<SortExec>().is_some())
        + plan.children().into_iter().map(count_sorts).sum::<usize>()
}

/// The keys of the sort directly below the window as (column, descending, nulls first).
fn input_sort_keys(window: &Arc<dyn ExecutionPlan>) -> Option<Vec<(usize, bool, bool)>> {
    let sort = window.children()[0].downcast_ref::<SortExec>()?;
    Some(
        sort.expr()
            .iter()
            .map(|e| {
                let column = e.expr.downcast_ref::<Column>().expect("a column sort key");
                (column.index(), e.options.descending, e.options.nulls_first)
            })
            .collect(),
    )
}

struct Case {
    name: &'static str,
    input_order: Option<&'static [(i32, bool)]>,
    partition_by: &'static [i32],
    order_by: &'static [i32],
    /// The keys of the sort the planner adds, as (column, descending, nulls first).
    added_sort: Option<Vec<(usize, bool, bool)>>,
}

#[test]
fn window_sorts_input_without_partition_key_order() {
    let asc = |index: usize| (index, false, true);
    let cases = [
        Case {
            name: "no declared order",
            input_order: None,
            partition_by: &[0],
            order_by: &[2],
            added_sort: Some(vec![asc(0), asc(2)]),
        },
        // The ORDER BY key is not declared either. Only the PARTITION BY keys are checked.
        Case {
            name: "ordered on the partition key",
            input_order: Some(&[(0, false)]),
            partition_by: &[0],
            order_by: &[2],
            added_sort: None,
        },
        Case {
            name: "ordered on the first of two partition keys",
            input_order: Some(&[(0, false)]),
            partition_by: &[0, 1],
            order_by: &[2],
            added_sort: Some(vec![asc(0), asc(1), asc(2)]),
        },
        Case {
            name: "ordered on the partition keys in another order",
            input_order: Some(&[(1, false), (0, false)]),
            partition_by: &[0, 1],
            order_by: &[2],
            added_sort: None,
        },
        Case {
            name: "ordered descending on the partition key",
            input_order: Some(&[(0, true)]),
            partition_by: &[0],
            order_by: &[2],
            added_sort: None,
        },
        Case {
            name: "no PARTITION BY",
            input_order: None,
            partition_by: &[],
            order_by: &[2],
            added_sort: None,
        },
    ];
    let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), 0);
    for (function, exec) in FUNCTIONS {
        for case in &cases {
            let child = match case.input_order {
                Some(order) => sort(scan(), order),
                None => scan(),
            };
            let operator = window(child, function, case.partition_by, case.order_by);
            let (_, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
            let window = find_window(&planned.native_plan);
            let context = format!("{function}, {}", case.name);
            assert_eq!(window.name(), exec, "{context}");
            assert_eq!(
                count_sorts(&window),
                usize::from(case.input_order.is_some()) + usize::from(case.added_sort.is_some()),
                "{context}"
            );
            if case.added_sort.is_some() {
                assert_eq!(input_sort_keys(&window), case.added_sort, "{context}");
            }
        }
    }
}

#[test]
fn window_keeps_input_sorted_on_a_float_partition_key() {
    // Both the sort key and the PARTITION BY key go through float normalization, so the window
    // sees the input as ordered on its key.
    let key = || typed_column(0, DataTypeId::Double);
    let planner = PhysicalPlanner::new(Arc::new(SessionContext::new()), 0);
    for (function, exec) in FUNCTIONS {
        let input = scan_of(vec![data_type(DataTypeId::Double), int32(), int32()]);
        let child = sort_by(input, vec![sort_order_of(key(), false)]);
        let order_by = vec![sort_order(2, false)];
        let operator = window_by(child, function, vec![key()], order_by, None);
        let (_, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
        let window = find_window(&planned.native_plan);
        assert_eq!(window.name(), exec, "{function}");
        assert_eq!(count_sorts(&window), 1, "{function}");
    }
}

/// Rows of (a, b, v) for the execution tests.
fn input_columns() -> (Vec<ArrayRef>, usize) {
    let a = vec![1, 0, 1, 0, 1];
    let v = vec![5, 4, 3, 2, 1];
    let rows = a.len();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(a)),
        Arc::new(Int32Array::from(vec![0; rows])),
        Arc::new(Int32Array::from(v)),
    ];
    (columns, rows)
}

#[tokio::test]
async fn window_reports_its_input_sort_metrics() {
    let session = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
    for (cast_result, sorted_input) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let context = format!("cast_result={cast_result} sorted_input={sorted_input}");
        let child = if sorted_input {
            sort(scan(), &[(0, false)])
        } else {
            scan()
        };
        // `row_number` returns UInt64 natively, so an Int32 result type adds a cast.
        let result_type = cast_result.then(int32);
        let partition_by = vec![column(0)];
        let order_by = vec![sort_order(2, false)];
        let operator = window_by(child, "row_number", partition_by, order_by, result_type);
        let (mut scans, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();

        let additional: Vec<&str> = planned
            .additional_native_plans
            .iter()
            .map(|p| p.name())
            .collect();
        let expected: &[&str] = match (cast_result, sorted_input) {
            (false, false) => &["SortExec"],
            (false, true) => &[],
            (true, false) => &["BoundedWindowAggExec", "SortExec"],
            (true, true) => &["BoundedWindowAggExec"],
        };
        assert_eq!(additional, expected, "{context}");

        let (columns, rows) = input_columns();
        let mut inputs = [vec![InputBatch::Batch(columns, rows), InputBatch::EOF].into_iter()];
        let stream = planned.native_plan.execute(0, session.task_ctx()).unwrap();
        let batches = collect_with_inputs(stream, &mut scans, &mut inputs).await;
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows,
            "{context}"
        );

        let elapsed = |plan: &Arc<dyn ExecutionPlan>| {
            plan.metrics()
                .and_then(|m| m.elapsed_compute())
                .unwrap_or(0) as i64
        };
        let total_time = std::iter::once(&planned.native_plan)
            .chain(&planned.additional_native_plans)
            .map(elapsed)
            .sum::<i64>();
        let metrics = to_native_metric_node(&planned).unwrap();
        // Output rows come from the top operator only, and the time covers all of them.
        assert_eq!(metrics.metrics["output_rows"], rows as i64, "{context}");
        assert_eq!(metrics.metrics["elapsed_compute"], total_time, "{context}");
    }
}

#[tokio::test]
async fn window_over_unordered_input_ranks_each_partition() {
    let session = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
    for (function, expected) in [
        ("row_number", [1.0, 2.0, 1.0, 2.0, 3.0]),
        ("percent_rank", [0.0, 1.0, 0.0, 0.5, 1.0]),
    ] {
        let operator = window(scan(), function, &[0], &[2]);
        let (mut scans, _, planned) = planner.create_plan(&operator, &mut vec![], 1).unwrap();
        let (columns, num_rows) = input_columns();
        let mut inputs = [vec![InputBatch::Batch(columns, num_rows), InputBatch::EOF].into_iter()];
        let stream = planned.native_plan.execute(0, session.task_ctx()).unwrap();
        let batches = collect_with_inputs(stream, &mut scans, &mut inputs).await;
        let mut rows = vec![];
        for batch in &batches {
            let ints = |i: usize| {
                let column = batch.column(i).as_any().downcast_ref::<Int32Array>();
                column.unwrap().clone()
            };
            let result = cast(batch.column(3), &DataType::Float64).unwrap();
            let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
            let (keys, values) = (ints(0), ints(2));
            for row in 0..batch.num_rows() {
                rows.push((keys.value(row), values.value(row), result.value(row)));
            }
        }
        rows.sort_by_key(|&(k, v, _)| (k, v));
        let ranks: Vec<f64> = rows.iter().map(|r| r.2).collect();
        assert_eq!(rows.len(), num_rows, "{function}");
        assert_eq!(ranks, expected, "{function}");
    }
}
