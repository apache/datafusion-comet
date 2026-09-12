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

use super::*;
use datafusion::physical_expr::expressions::lit;

mod timestamp;

/// Accept nested null-check conjunctions while retaining all other filter
/// boundaries, including OR and computed or potentially failing expressions.
#[test]
fn reader_filter_crosses_only_direct_column_null_checks() {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let direct_null_check: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(Arc::clone(&key)));
    assert!(is_direct_column_null_checks(&direct_null_check));
    let other_null_check: Arc<dyn PhysicalExpr> =
        Arc::new(IsNotNullExpr::new(Arc::new(Column::new("other", 1))));
    let conjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        Arc::clone(&other_null_check),
    ));
    let nested: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        conjunction,
        Operator::And,
        Arc::clone(&direct_null_check),
    ));
    assert!(is_direct_column_null_checks(&nested));
    let right_nested: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        Arc::new(BinaryExpr::new(
            Arc::clone(&other_null_check),
            Operator::And,
            Arc::clone(&direct_null_check),
        )),
    ));
    assert!(is_direct_column_null_checks(&right_nested));
    let disjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::Or,
        other_null_check,
    ));
    assert!(!is_direct_column_null_checks(&disjunction));

    let comparison: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(Arc::clone(&key), Operator::Gt, lit(0_i32)));
    let conjunction: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&direct_null_check),
        Operator::And,
        comparison,
    ));
    assert!(!is_direct_column_null_checks(&conjunction));

    let computed: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(key, Operator::Plus, lit(1_i32)));
    let computed_null_check: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(computed));
    assert!(!is_direct_column_null_checks(&computed_null_check));
}
