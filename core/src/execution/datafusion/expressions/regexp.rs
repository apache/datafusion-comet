/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::execution::datafusion::expressions::{
    strings::{StringSpaceExec, SubstringExec},
    utils::down_cast_any_ref,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hasher,
    sync::Arc,
};

#[derive(Debug, Hash)]
pub struct RLike {
    child: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl RLike {
    pub fn new(child: Arc<dyn PhysicalExpr>, pattern: Arc<dyn PhysicalExpr>) -> Self {
        Self { child, pattern }
    }
}

impl Display for RLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RLike [child: {}, pattern: {}] ",
            self.child, self.pattern
        )
    }
}

impl PartialEq<dyn Any> for RLike {
    fn eq(&self, other: &dyn Any) -> bool {
        todo!()
    }
}

impl PhysicalExpr for RLike {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        todo!()
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion_common::Result<bool> {
        todo!()
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn dyn_hash(&self, _state: &mut dyn Hasher) {
        todo!()
    }
}
