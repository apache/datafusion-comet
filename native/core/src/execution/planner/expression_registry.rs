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

//! Expression registry for dispatching expression creation

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::{expr::ExprStruct, Expr};

use crate::execution::operators::ExecutionError;
use crate::execution::planner::traits::{ExpressionBuilder, ExpressionType};

/// Registry for expression builders
pub struct ExpressionRegistry {
    builders: HashMap<ExpressionType, Box<dyn ExpressionBuilder>>,
}

impl ExpressionRegistry {
    /// Create a new expression registry with all builders registered
    pub fn new() -> Self {
        let mut registry = Self {
            builders: HashMap::new(),
        };

        registry.register_all_expressions();
        registry
    }

    /// Check if the registry can handle a given expression type
    pub fn can_handle(&self, spark_expr: &Expr) -> bool {
        if let Ok(expr_type) = Self::get_expression_type(spark_expr) {
            self.builders.contains_key(&expr_type)
        } else {
            false
        }
    }

    /// Create a physical expression from a Spark protobuf expression
    pub fn create_expr(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &super::PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        let expr_type = Self::get_expression_type(spark_expr)?;

        if let Some(builder) = self.builders.get(&expr_type) {
            builder.build(spark_expr, input_schema, planner)
        } else {
            Err(ExecutionError::GeneralError(format!(
                "No builder registered for expression type: {:?}",
                expr_type
            )))
        }
    }

    /// Register all expression builders
    fn register_all_expressions(&mut self) {
        // Register arithmetic expressions
        self.register_arithmetic_expressions();

        // TODO: Register other expression categories in future phases
        // self.register_comparison_expressions();
        // self.register_string_expressions();
        // self.register_temporal_expressions();
        // etc.
    }

    /// Register arithmetic expression builders
    fn register_arithmetic_expressions(&mut self) {
        use crate::execution::expressions::arithmetic::*;

        self.builders
            .insert(ExpressionType::Add, Box::new(AddBuilder));
        self.builders
            .insert(ExpressionType::Subtract, Box::new(SubtractBuilder));
        self.builders
            .insert(ExpressionType::Multiply, Box::new(MultiplyBuilder));
        self.builders
            .insert(ExpressionType::Divide, Box::new(DivideBuilder));
        self.builders.insert(
            ExpressionType::IntegralDivide,
            Box::new(IntegralDivideBuilder),
        );
        self.builders
            .insert(ExpressionType::Remainder, Box::new(RemainderBuilder));
        self.builders
            .insert(ExpressionType::UnaryMinus, Box::new(UnaryMinusBuilder));
    }

    /// Extract expression type from Spark protobuf expression
    fn get_expression_type(spark_expr: &Expr) -> Result<ExpressionType, ExecutionError> {
        match spark_expr.expr_struct.as_ref() {
            Some(ExprStruct::Add(_)) => Ok(ExpressionType::Add),
            Some(ExprStruct::Subtract(_)) => Ok(ExpressionType::Subtract),
            Some(ExprStruct::Multiply(_)) => Ok(ExpressionType::Multiply),
            Some(ExprStruct::Divide(_)) => Ok(ExpressionType::Divide),
            Some(ExprStruct::IntegralDivide(_)) => Ok(ExpressionType::IntegralDivide),
            Some(ExprStruct::Remainder(_)) => Ok(ExpressionType::Remainder),
            Some(ExprStruct::UnaryMinus(_)) => Ok(ExpressionType::UnaryMinus),

            Some(ExprStruct::Eq(_)) => Ok(ExpressionType::Eq),
            Some(ExprStruct::Neq(_)) => Ok(ExpressionType::Neq),
            Some(ExprStruct::Lt(_)) => Ok(ExpressionType::Lt),
            Some(ExprStruct::LtEq(_)) => Ok(ExpressionType::LtEq),
            Some(ExprStruct::Gt(_)) => Ok(ExpressionType::Gt),
            Some(ExprStruct::GtEq(_)) => Ok(ExpressionType::GtEq),
            Some(ExprStruct::EqNullSafe(_)) => Ok(ExpressionType::EqNullSafe),
            Some(ExprStruct::NeqNullSafe(_)) => Ok(ExpressionType::NeqNullSafe),

            Some(ExprStruct::And(_)) => Ok(ExpressionType::And),
            Some(ExprStruct::Or(_)) => Ok(ExpressionType::Or),
            Some(ExprStruct::Not(_)) => Ok(ExpressionType::Not),

            Some(ExprStruct::IsNull(_)) => Ok(ExpressionType::IsNull),
            Some(ExprStruct::IsNotNull(_)) => Ok(ExpressionType::IsNotNull),

            Some(ExprStruct::BitwiseAnd(_)) => Ok(ExpressionType::BitwiseAnd),
            Some(ExprStruct::BitwiseOr(_)) => Ok(ExpressionType::BitwiseOr),
            Some(ExprStruct::BitwiseXor(_)) => Ok(ExpressionType::BitwiseXor),
            Some(ExprStruct::BitwiseShiftLeft(_)) => Ok(ExpressionType::BitwiseShiftLeft),
            Some(ExprStruct::BitwiseShiftRight(_)) => Ok(ExpressionType::BitwiseShiftRight),

            Some(ExprStruct::Bound(_)) => Ok(ExpressionType::Bound),
            Some(ExprStruct::Unbound(_)) => Ok(ExpressionType::Unbound),
            Some(ExprStruct::Literal(_)) => Ok(ExpressionType::Literal),
            Some(ExprStruct::Cast(_)) => Ok(ExpressionType::Cast),
            Some(ExprStruct::CaseWhen(_)) => Ok(ExpressionType::CaseWhen),
            Some(ExprStruct::In(_)) => Ok(ExpressionType::In),
            Some(ExprStruct::If(_)) => Ok(ExpressionType::If),
            Some(ExprStruct::Substring(_)) => Ok(ExpressionType::Substring),
            Some(ExprStruct::Like(_)) => Ok(ExpressionType::Like),
            Some(ExprStruct::Rlike(_)) => Ok(ExpressionType::Rlike),
            Some(ExprStruct::CheckOverflow(_)) => Ok(ExpressionType::CheckOverflow),
            Some(ExprStruct::ScalarFunc(_)) => Ok(ExpressionType::ScalarFunc),
            Some(ExprStruct::NormalizeNanAndZero(_)) => Ok(ExpressionType::NormalizeNanAndZero),
            Some(ExprStruct::Subquery(_)) => Ok(ExpressionType::Subquery),
            Some(ExprStruct::BloomFilterMightContain(_)) => {
                Ok(ExpressionType::BloomFilterMightContain)
            }
            Some(ExprStruct::CreateNamedStruct(_)) => Ok(ExpressionType::CreateNamedStruct),
            Some(ExprStruct::GetStructField(_)) => Ok(ExpressionType::GetStructField),
            Some(ExprStruct::ToJson(_)) => Ok(ExpressionType::ToJson),
            Some(ExprStruct::ToPrettyString(_)) => Ok(ExpressionType::ToPrettyString),
            Some(ExprStruct::ListExtract(_)) => Ok(ExpressionType::ListExtract),
            Some(ExprStruct::GetArrayStructFields(_)) => Ok(ExpressionType::GetArrayStructFields),
            Some(ExprStruct::ArrayInsert(_)) => Ok(ExpressionType::ArrayInsert),
            Some(ExprStruct::Rand(_)) => Ok(ExpressionType::Rand),
            Some(ExprStruct::Randn(_)) => Ok(ExpressionType::Randn),
            Some(ExprStruct::SparkPartitionId(_)) => Ok(ExpressionType::SparkPartitionId),
            Some(ExprStruct::MonotonicallyIncreasingId(_)) => {
                Ok(ExpressionType::MonotonicallyIncreasingId)
            }

            Some(ExprStruct::Hour(_)) => Ok(ExpressionType::Hour),
            Some(ExprStruct::Minute(_)) => Ok(ExpressionType::Minute),
            Some(ExprStruct::Second(_)) => Ok(ExpressionType::Second),
            Some(ExprStruct::TruncTimestamp(_)) => Ok(ExpressionType::TruncTimestamp),

            Some(other) => Err(ExecutionError::GeneralError(format!(
                "Unsupported expression type: {:?}",
                other
            ))),
            None => Err(ExecutionError::GeneralError(
                "Expression struct is None".to_string(),
            )),
        }
    }
}

impl Default for ExpressionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
