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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::scan::CometScanExec;

/// A DataFusion `TableProvider` that produces a `CometScanExec`. Carries the
/// Comet proto so the table can be reconstructed on the scheduler side via the
/// logical codec below.
#[derive(Debug)]
pub struct CometTableProvider {
    proto: Vec<u8>,
    schema: SchemaRef,
}

impl CometTableProvider {
    pub fn new(proto: Vec<u8>, schema: SchemaRef) -> Self {
        Self { proto, schema }
    }
    pub fn proto(&self) -> &[u8] {
        &self.proto
    }
}

#[async_trait::async_trait]
impl TableProvider for CometTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    // `_filters` and `_limit` are intentionally not pushed down into the Comet
    // scan; DataFusion re-applies them on top of the returned plan.
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan: Arc<dyn ExecutionPlan> = Arc::new(CometScanExec::try_new(self.proto.clone())?);
        match projection {
            Some(indices) => {
                let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = indices
                    .iter()
                    .map(|&i| {
                        let f = self.schema.field(i);
                        (
                            Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                            f.name().to_string(),
                        )
                    })
                    .collect();
                Ok(Arc::new(ProjectionExec::try_new(exprs, scan)?))
            }
            None => Ok(scan),
        }
    }
}
