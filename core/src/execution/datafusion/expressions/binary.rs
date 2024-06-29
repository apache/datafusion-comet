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
use std::fmt::{Display, Formatter};
use std::hash::Hasher;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr_common::physical_expr::{down_cast_any_ref, PhysicalExpr};

use crate::execution::datafusion::expressions::EvalMode;

#[derive(Debug, Hash, Clone)]
pub struct CometBinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    eval_mode: EvalMode,
    inner: Arc<BinaryExpr>,
}

impl CometBinaryExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            left: Arc::clone(&left),
            op,
            right: Arc::clone(&right),
            eval_mode,
            inner: Arc::new(BinaryExpr::new(left, op, right)),
        }
    }
}

impl Display for CometBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CometBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // TODO: Do some work here
        self.inner.evaluate(batch)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        self.inner.evaluate_selection(batch, selection)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Arc::clone(&self.inner).with_new_children(children)
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, children)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        self.inner.dyn_hash(state)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        self.inner.get_properties(children)
    }
}

impl PartialEq<dyn Any> for CometBinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.left.eq(&x.left) && self.op == x.op && self.right.eq(&x.right))
            .unwrap_or(false)
    }
}