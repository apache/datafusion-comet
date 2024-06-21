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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::expressions::BinaryExpr;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;

#[derive(Debug, Hash, Clone)]
pub struct ModuloExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl ModuloExpr {
    pub fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            left,
            op: Operator::Modulo,
            right,
        }
    }
}

impl fmt::Display for ModuloExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} % {}", self.left, self.right)
    }
}

impl PartialEq<dyn Any> for ModuloExpr {
    fn eq(&self, other: &(dyn Any + 'static)) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.left.eq(&other.left) && self.op == other.op && self.right.eq(&other.right)
        } else {
            false
        }
    }
}

impl PhysicalExpr for ModuloExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rhs = self.right.evaluate(batch)?;

        // Following to match spark's behavior for modulo with -0.0
        match rhs {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) => {
                if val == -0.0 {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                }
            }
            ColumnarValue::Array(arr) => {
                if arr.data_type() == &arrow::datatypes::DataType::Float64 {
                    let float64_array = arr
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap();

                    for i in 0..float64_array.len() {
                        if float64_array.value(i) == -0.0 {
                            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                        }
                    }
                }
            }
            _ => {}
        }
        // Otherwise, use the BinaryExpr implementation for modulo
        let binary_expr = BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right));
        binary_expr.evaluate(batch)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &arrow_array::BooleanArray,
    ) -> Result<ColumnarValue> {
        let rhs = self.right.evaluate_selection(batch, selection)?;

        // Check if the right operand is a ScalarValue of Float64 and equal to -0.0
        match rhs {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) => {
                if val == -0.0 {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                }
            }
            ColumnarValue::Array(arr) => {
                if arr.data_type() == &arrow::datatypes::DataType::Float64 {
                    let float64_array = arr
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap();

                    for i in 0..float64_array.len() {
                        if float64_array.value(i) == -0.0 {
                            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                        }
                    }
                }
            }
            _ => {}
        }

        // Otherwise, use the BinaryExpr implementation for modulo
        let binary_expr = BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right));
        binary_expr.evaluate_selection(batch, selection)
    }

    fn evaluate_bounds(
        &self,
        _children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<datafusion_expr::interval_arithmetic::Interval> {
        datafusion_common::not_impl_err!("Not implemented for {self}")
    }

    fn propagate_constraints(
        &self,
        _interval: &datafusion_expr::interval_arithmetic::Interval,
        _children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<Option<Vec<datafusion_expr::interval_arithmetic::Interval>>> {
        Ok(Some(vec![]))
    }

    fn get_properties(
        &self,
        _children: &[datafusion_expr::sort_properties::ExprProperties],
    ) -> Result<datafusion_expr::sort_properties::ExprProperties> {
        Ok(datafusion_expr::sort_properties::ExprProperties::new_unknown())
    }

    fn data_type(&self, input_schema: &Schema) -> Result<arrow_schema::DataType> {
        let binary_expr = BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right));
        binary_expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let binary_expr = BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right));
        binary_expr.nullable(input_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() == 2 {
            Ok(Arc::new(ModuloExpr::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
            )))
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "Invalid number of children".to_string(),
            ))
        }
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let binary_expr = BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right));
        binary_expr.dyn_hash(state)
    }
}
