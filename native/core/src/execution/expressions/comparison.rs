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

//! Comparison expression builders

use datafusion::logical_expr::Operator as DataFusionOperator;

use crate::binary_expr_builder;

/// Macro to define all comparison builders at once
macro_rules! define_comparison_builders {
    ($(($builder:ident, $expr_type:ident, $op:expr)),* $(,)?) => {
        $(
            binary_expr_builder!($builder, $expr_type, $op);
        )*
    };
}

define_comparison_builders![
    (EqBuilder, Eq, DataFusionOperator::Eq),
    (NeqBuilder, Neq, DataFusionOperator::NotEq),
    (LtBuilder, Lt, DataFusionOperator::Lt),
    (LtEqBuilder, LtEq, DataFusionOperator::LtEq),
    (GtBuilder, Gt, DataFusionOperator::Gt),
    (GtEqBuilder, GtEq, DataFusionOperator::GtEq),
    (
        EqNullSafeBuilder,
        EqNullSafe,
        DataFusionOperator::IsNotDistinctFrom
    ),
    (
        NeqNullSafeBuilder,
        NeqNullSafe,
        DataFusionOperator::IsDistinctFrom
    ),
];
