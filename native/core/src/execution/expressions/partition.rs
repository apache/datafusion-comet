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

use crate::execution::operators::ExecutionError;
use crate::execution::planner::expression_registry::ExpressionBuilder;
use crate::execution::planner::PhysicalPlanner;
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::monotonically_increasing_id::MonotonicallyIncreasingId;
use std::sync::Arc;

pub struct SparkPartitionIdBuilder;

impl ExpressionBuilder for SparkPartitionIdBuilder {
    fn build(
        &self,
        _spark_expr: &Expr,
        _input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        Ok(Arc::new(Literal::new(ScalarValue::Int32(Some(
            planner.partition(),
        )))))
    }
}

pub struct MonotonicallyIncreasingIdBuilder;

impl ExpressionBuilder for MonotonicallyIncreasingIdBuilder {
    fn build(
        &self,
        _spark_expr: &Expr,
        _input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        Ok(Arc::new(MonotonicallyIncreasingId::from_partition_id(
            planner.partition(),
        )))
    }
}
