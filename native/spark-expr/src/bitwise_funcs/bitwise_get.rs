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
use std::hash::Hash;
use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::internal_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::common::Result;

/// BitwiseNot expression
#[derive(Debug, Eq)]
pub struct BitwiseGetExpr {
    arg_expr: Arc<dyn PhysicalExpr>,
    pos_expr: Arc<dyn PhysicalExpr>,
}

impl Hash for BitwiseGetExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg_expr.hash(state);
        self.pos_expr.hash(state);
    }
}

impl PartialEq for BitwiseGetExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg_expr.eq(&other.arg_expr) && self.pos_expr.eq(&other.pos_expr)
    }
}

impl BitwiseGetExpr {
    /// Create new bitwise get expression
    pub fn new(arg_expr: Arc<dyn PhysicalExpr>, pos_expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg_expr, pos_expr }
    }
}

impl std::fmt::Display for BitwiseGetExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}, position: {}", self.arg_expr, self.pos_expr)
    }
}

impl PhysicalExpr for BitwiseGetExpr {
    fn as_any(&self) -> &dyn Any { self }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg_expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg_expr.nullable(input_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg_expr, &self.pos_expr]
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {

        Ok(ColumnarValue::Array())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(BitwiseGetExpr::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]))
            )),
            _ => internal_err!("BitwiseGet should have exactly two childrens"),
        }
    }
}

pub fn bitwise_get(arg_expr: Arc<dyn PhysicalExpr>, pos_expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(BitwiseGetExpr::new(arg_expr, pos_expr)))
}