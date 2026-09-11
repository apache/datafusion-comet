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

//! Keep per-file schema adaptation errors visible when reader filters are attached.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};

/// Owned by one execution's scan; the wrapped adapter below is created separately for each file.
#[derive(Debug)]
pub(super) struct ReaderFilterAdapterFactory {
    inner: Arc<dyn PhysicalExprAdapterFactory>,
    read_file_columns: Vec<usize>,
}

impl ReaderFilterAdapterFactory {
    pub(super) fn new(
        inner: Arc<dyn PhysicalExprAdapterFactory>,
        read_file_columns: Vec<usize>,
    ) -> Self {
        Self {
            inner,
            read_file_columns,
        }
    }
}

impl PhysicalExprAdapterFactory for ReaderFilterAdapterFactory {
    fn create(
        &self,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let inner = self
            .inner
            .create(Arc::clone(&logical_schema), physical_schema)?;
        // DataFusion rewrites predicates before projections. Check projected and
        // statically filtered file columns before a threshold can discard all groups.
        // Column remapping and missing/default literals are infallible. Other adaptations
        // may reject nonempty batches (e.g. Spark 3 INT32 -> BIGINT), so retain their rows.
        let mut allow_dynamic_filter = true;
        for &index in &self.read_file_columns {
            let column = Arc::new(Column::new(logical_schema.field(index).name(), index));
            match inner.rewrite(column) {
                Ok(expr) if expr.is::<Column>() || expr.is::<Literal>() => {}
                _ => {
                    allow_dynamic_filter = false;
                    break;
                }
            }
        }
        Ok(Arc::new(ReaderFilterAdapter {
            inner,
            allow_dynamic_filter,
        }))
    }
}

#[derive(Debug)]
struct ReaderFilterAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    allow_dynamic_filter: bool,
}

impl PhysicalExprAdapter for ReaderFilterAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = if self.allow_dynamic_filter {
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
        // Preserve static filters and the normal rejection timing: empty files and files
        // excluded by Spark's own predicates must not acquire eager conversion failures.
        self.inner.rewrite(expr)
    }
}
