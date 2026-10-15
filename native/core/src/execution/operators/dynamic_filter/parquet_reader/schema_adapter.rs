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

//! Preserve per-file conversion errors when runtime reader filters are attached.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};

/// Wrap one execution's scan adapter factory, checking conversions separately for each file.
#[derive(Debug)]
pub(super) struct RuntimeFilterSchemaAdapterFactory {
    inner: Arc<dyn PhysicalExprAdapterFactory>,
    read_columns: Vec<Column>,
}

impl RuntimeFilterSchemaAdapterFactory {
    pub(super) fn new(
        inner: Arc<dyn PhysicalExprAdapterFactory>,
        read_columns: Vec<Column>,
    ) -> Self {
        Self {
            inner,
            read_columns,
        }
    }
}

impl PhysicalExprAdapterFactory for RuntimeFilterSchemaAdapterFactory {
    fn create(
        &self,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let inner = self
            .inner
            .create(logical_schema, Arc::clone(&physical_schema))?;
        // DataFusion adapts predicates before projections and row-group pruning.
        // Direct remapping and missing/default literals are safe. Other adaptations
        // can reject nonempty batches or overflow, so let normal decoding run first.
        // Spark's adapter can leave unresolved columns unchanged. Require rewritten
        // column names to resolve in the original physical schema; case and field-ID
        // remapping restore those names before returning the expression.
        // Safe-adaptation and matching-schema optimizations are tracked in
        // https://github.com/apache/datafusion-comet/issues/6123.
        let allow_runtime_filter =
            self.read_columns
                .iter()
                .all(|column| match inner.rewrite(Arc::new(column.clone())) {
                    Ok(expr) if expr.is::<Literal>() => true,
                    Ok(expr) => expr
                        .downcast_ref::<Column>()
                        .is_some_and(|column| physical_schema.index_of(column.name()).is_ok()),
                    Err(_) => false,
                });
        Ok(Arc::new(RuntimeFilterSchemaAdapter {
            inner,
            allow_runtime_filter,
        }))
    }
}

#[derive(Debug)]
struct RuntimeFilterSchemaAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    allow_runtime_filter: bool,
}

impl PhysicalExprAdapter for RuntimeFilterSchemaAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = if self.allow_runtime_filter {
            expr
        } else {
            expr.transform_down(|expr| {
                if expr.is::<DynamicFilterPhysicalExpr>() {
                    Ok(Transformed::yes(lit(true)))
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .data()?
        };
        // Keep static predicates and normal error timing: empty or statically
        // excluded files must not acquire eager conversion failures.
        self.inner.rewrite(expr)
    }
}

#[cfg(test)]
mod tests;
