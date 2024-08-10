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

// TODO upstream this to DataFusion as long as we have a way to specify all
// of the Spark-specific compatibility features that we need

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// to_json function
#[derive(Debug, Hash)]
pub struct ToJson {
    /// The input to convert to JSON
    expr: Arc<dyn PhysicalExpr>, //TODO options such as null handling
}

impl ToJson {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl Display for ToJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO options
        write!(f, "to_json({})", self.expr)
    }
}

impl PartialEq<dyn Any> for ToJson {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ToJson>() {
            //TODO compare options
            self.expr.eq(&other.expr)
        } else {
            false
        }
    }
}

impl PhysicalExpr for ToJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(to_json(input.as_ref())?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        // TODO options
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        // TODO options
        let mut s = state;
        self.expr.hash(&mut s);
        self.hash(&mut s);
    }
}

fn to_json(array: &dyn Array) -> Result<ArrayRef> {
    todo!()
}
