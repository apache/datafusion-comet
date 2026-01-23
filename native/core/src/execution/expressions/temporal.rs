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

//! Temporal expression builders

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_comet_proto::spark_expression::Expr;
use datafusion_comet_spark_expr::{
    SparkHour, SparkMinute, SparkSecond, SparkUnixTimestamp, TimestampTruncExpr,
};

use crate::execution::{
    expressions::extract_expr,
    operators::ExecutionError,
    planner::{expression_registry::ExpressionBuilder, PhysicalPlanner},
};

pub struct HourBuilder;

impl ExpressionBuilder for HourBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Hour);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let timezone = expr.timezone.clone();
        let args = vec![child];
        let comet_hour = Arc::new(ScalarUDF::new_from_impl(SparkHour::new(timezone)));
        let field_ref = Arc::new(Field::new("hour", DataType::Int32, true));
        let expr: ScalarFunctionExpr = ScalarFunctionExpr::new(
            "hour",
            comet_hour,
            args,
            field_ref,
            Arc::new(ConfigOptions::default()),
        );

        Ok(Arc::new(expr))
    }
}

pub struct MinuteBuilder;

impl ExpressionBuilder for MinuteBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Minute);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let timezone = expr.timezone.clone();
        let args = vec![child];
        let comet_minute = Arc::new(ScalarUDF::new_from_impl(SparkMinute::new(timezone)));
        let field_ref = Arc::new(Field::new("minute", DataType::Int32, true));
        let expr: ScalarFunctionExpr = ScalarFunctionExpr::new(
            "minute",
            comet_minute,
            args,
            field_ref,
            Arc::new(ConfigOptions::default()),
        );

        Ok(Arc::new(expr))
    }
}

pub struct SecondBuilder;

impl ExpressionBuilder for SecondBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, Second);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let timezone = expr.timezone.clone();
        let args = vec![child];
        let comet_second = Arc::new(ScalarUDF::new_from_impl(SparkSecond::new(timezone)));
        let field_ref = Arc::new(Field::new("second", DataType::Int32, true));
        let expr: ScalarFunctionExpr = ScalarFunctionExpr::new(
            "second",
            comet_second,
            args,
            field_ref,
            Arc::new(ConfigOptions::default()),
        );

        Ok(Arc::new(expr))
    }
}

pub struct UnixTimestampBuilder;

impl ExpressionBuilder for UnixTimestampBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, UnixTimestamp);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let timezone = expr.timezone.clone();
        let args = vec![child];
        let comet_unix_timestamp =
            Arc::new(ScalarUDF::new_from_impl(SparkUnixTimestamp::new(timezone)));
        let field_ref = Arc::new(Field::new("unix_timestamp", DataType::Int64, true));
        let expr: ScalarFunctionExpr = ScalarFunctionExpr::new(
            "unix_timestamp",
            comet_unix_timestamp,
            args,
            field_ref,
            Arc::new(ConfigOptions::default()),
        );

        Ok(Arc::new(expr))
    }
}

pub struct TruncTimestampBuilder;

impl ExpressionBuilder for TruncTimestampBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr = extract_expr!(spark_expr, TruncTimestamp);
        let child = planner.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let format = planner.create_expr(expr.format.as_ref().unwrap(), input_schema)?;
        let timezone = expr.timezone.clone();

        Ok(Arc::new(TimestampTruncExpr::new(child, format, timezone)))
    }
}
