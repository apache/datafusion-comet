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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

use crate::{QueryContext, SparkError, SparkErrorWithContext};

/// Wrapper expression that catches and wraps SparkError with QueryContext
/// for binary arithmetic operations.
#[derive(Debug)]
pub struct CheckedBinaryExpr {
    /// The underlying physical expression (typically a ScalarFunctionExpr)
    child: Arc<dyn PhysicalExpr>,
    /// Optional query context to attach to errors
    query_context: Option<Arc<QueryContext>>,
}

impl CheckedBinaryExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, query_context: Option<Arc<QueryContext>>) -> Self {
        Self {
            child,
            query_context,
        }
    }

    pub fn child(&self) -> &Arc<dyn PhysicalExpr> {
        &self.child
    }
}

impl Display for CheckedBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CheckedBinaryExpr({})", self.child)
    }
}

impl PartialEq for CheckedBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for CheckedBinaryExpr {}

impl PartialEq<dyn Any> for CheckedBinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self.eq(x))
            .unwrap_or(false)
    }
}

impl Hash for CheckedBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for CheckedBinaryExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.child.fmt_sql(f)
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let result = self.child.evaluate(batch);

        // If there's an error and we have query_context, wrap it
        match result {
            Err(DataFusionError::External(e)) if self.query_context.is_some() => {
                if let Some(spark_err) = e.downcast_ref::<SparkError>() {
                    let wrapped = SparkErrorWithContext::with_context(
                        spark_err.clone(),
                        Arc::clone(self.query_context.as_ref().unwrap()),
                    );
                    Err(DataFusionError::External(Box::new(wrapped)))
                } else {
                    Err(DataFusionError::External(e))
                }
            }
            other => other,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(CheckedBinaryExpr::new(
                Arc::clone(&children[0]),
                self.query_context.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "CheckedBinaryExpr should have exactly one child".to_string(),
            )),
        }
    }
}
