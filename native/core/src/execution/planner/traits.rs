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

//! Core traits for the modular planner framework

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use jni::objects::GlobalRef;

use crate::execution::operators::ScanExec;
use crate::execution::{operators::ExecutionError, spark_plan::SparkPlan};

/// Trait for building physical expressions from Spark protobuf expressions
pub trait ExpressionBuilder: Send + Sync {
    /// Build a DataFusion physical expression from a Spark protobuf expression
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &super::PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError>;
}

/// Trait for building physical operators from Spark protobuf operators
#[allow(dead_code)]
pub trait OperatorBuilder: Send + Sync {
    /// Build a Spark plan from a protobuf operator
    fn build(
        &self,
        spark_plan: &datafusion_comet_proto::spark_operator::Operator,
        inputs: &mut Vec<Arc<GlobalRef>>,
        partition_count: usize,
        planner: &super::PhysicalPlanner,
    ) -> Result<(Vec<ScanExec>, Arc<SparkPlan>), ExecutionError>;
}

/// Enum to identify different expression types for registry dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpressionType {
    // Arithmetic expressions
    Add,
    Subtract,
    Multiply,
    Divide,
    IntegralDivide,
    Remainder,
    UnaryMinus,

    // Comparison expressions
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    EqNullSafe,
    NeqNullSafe,

    // Logical expressions
    And,
    Or,
    Not,

    // Null checks
    IsNull,
    IsNotNull,

    // Bitwise operations
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,

    // Other expressions
    Bound,
    Unbound,
    Literal,
    Cast,
    CaseWhen,
    In,
    If,
    Substring,
    Like,
    Rlike,
    CheckOverflow,
    ScalarFunc,
    NormalizeNanAndZero,
    Subquery,
    BloomFilterMightContain,
    CreateNamedStruct,
    GetStructField,
    ToJson,
    ToPrettyString,
    ListExtract,
    GetArrayStructFields,
    ArrayInsert,
    Rand,
    Randn,
    SparkPartitionId,
    MonotonicallyIncreasingId,

    // Time functions
    Hour,
    Minute,
    Second,
    TruncTimestamp,
}

/// Enum to identify different operator types for registry dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum OperatorType {
    Scan,
    NativeScan,
    IcebergScan,
    Projection,
    Filter,
    HashAgg,
    Limit,
    Sort,
    ShuffleWriter,
    ParquetWriter,
    Expand,
    SortMergeJoin,
    HashJoin,
    Window,
}
