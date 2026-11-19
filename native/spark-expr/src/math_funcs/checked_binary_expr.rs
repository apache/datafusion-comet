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
use datafusion_comet_common::error_chain;

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
        match self.child.evaluate(batch) {
            Err(e) if self.query_context.is_some() => {
                let spark_err = error_chain(&e)
                    .find_map(|e| e.downcast_ref::<SparkError>())
                    .cloned();
                match spark_err {
                    Some(spark_err) => {
                        let wrapped = SparkErrorWithContext::with_context(
                            spark_err,
                            Arc::clone(self.query_context.as_ref().unwrap()),
                        );
                        Err(DataFusionError::External(Box::new(wrapped)))
                    }
                    None => Err(e),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    /// Child expression whose `evaluate` always fails with `make_err()`. Instances compare by
    /// identity, which is all the `PhysicalExpr` bounds need here.
    #[derive(Debug)]
    struct FailingExpr {
        make_err: fn() -> DataFusionError,
    }

    impl Display for FailingExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingExpr")
        }
    }

    impl PartialEq for FailingExpr {
        fn eq(&self, other: &Self) -> bool {
            std::ptr::eq(self, other)
        }
    }

    impl Eq for FailingExpr {}

    impl PartialEq<dyn Any> for FailingExpr {
        fn eq(&self, other: &dyn Any) -> bool {
            other
                .downcast_ref::<Self>()
                .map(|x| self.eq(x))
                .unwrap_or(false)
        }
    }

    impl Hash for FailingExpr {
        fn hash<H: Hasher>(&self, state: &mut H) {
            std::ptr::hash(self, state);
        }
    }

    impl PhysicalExpr for FailingExpr {
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingExpr")
        }

        fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int32)
        }

        fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
            Ok(true)
        }

        fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
            Err((self.make_err)())
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
    }

    fn query_context(sql: &str) -> Arc<QueryContext> {
        Arc::new(QueryContext::new(sql.to_string(), 0, 4, None, None, 1, 0))
    }

    /// Evaluates a `CheckedBinaryExpr` carrying `ctx` over a child that fails with `make_err()`.
    fn evaluate_err(make_err: fn() -> DataFusionError, ctx: &Arc<QueryContext>) -> DataFusionError {
        let expr =
            CheckedBinaryExpr::new(Arc::new(FailingExpr { make_err }), Some(Arc::clone(ctx)));
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])));
        expr.evaluate(&batch).expect_err("child fails")
    }

    /// The `SparkErrorWithContext` the result carries directly in `External`.
    fn with_context(err: &DataFusionError) -> &SparkErrorWithContext {
        match err {
            DataFusionError::External(e) => e
                .downcast_ref::<SparkErrorWithContext>()
                .unwrap_or_else(|| panic!("expected SparkErrorWithContext, got {err:?}")),
            other => panic!("expected External, got {other:?}"),
        }
    }

    #[test]
    fn attaches_query_context_to_wrapped_spark_error() {
        let ctx = query_context("1 / 0");
        let err = evaluate_err(
            || DataFusionError::from(SparkError::DivideByZero).context("evaluating child"),
            &ctx,
        );
        let wrapped = with_context(&err);
        assert!(matches!(wrapped.error, SparkError::DivideByZero));
        assert!(Arc::ptr_eq(wrapped.context.as_ref().unwrap(), &ctx));
    }

    #[test]
    fn keeps_inner_query_context() {
        let outer = query_context("outer");
        let err = evaluate_err(
            || {
                DataFusionError::from(SparkErrorWithContext::with_context(
                    SparkError::DivideByZero,
                    query_context("inner"),
                ))
            },
            &outer,
        );
        let wrapped = with_context(&err);
        assert!(matches!(wrapped.error, SparkError::DivideByZero));
        assert_eq!(wrapped.context.as_ref().unwrap().sql_text.as_str(), "inner");
    }
}
