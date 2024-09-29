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

use crate::{spark_cast, EvalMode};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// to_string function
#[derive(Debug, Hash)]
pub struct ToString {
    /// The input to convert to string
    expr: Arc<dyn PhysicalExpr>,
    /// Timezone to use when converting timestamps to string
    timezone: String,
}

impl ToString {
    pub fn new(expr: Arc<dyn PhysicalExpr>, timezone: &str) -> Self {
        Self {
            expr,
            timezone: timezone.to_owned(),
        }
    }
}

impl Display for ToString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "to_string({}, timezone={})", self.expr, self.timezone)
    }
}

impl PartialEq<dyn Any> for ToString {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ToString>() {
            self.expr.eq(&other.expr) && self.timezone.eq(&other.timezone)
        } else {
            false
        }
    }
}

impl PhysicalExpr for ToString {
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
        Ok(ColumnarValue::Array(array_to_string(
            &input,
            &self.timezone,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            &self.timezone,
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.expr.hash(&mut s);
        self.timezone.hash(&mut s);
        self.hash(&mut s);
    }
}

/// Convert an array into a string representation
fn array_to_string(arr: &Arc<dyn Array>, timezone: &str) -> Result<ArrayRef> {
    spark_cast(
        ColumnarValue::Array(Arc::clone(arr)),
        &DataType::Utf8,
        EvalMode::Legacy,
        timezone,
        false,
    )?
        .into_array(arr.len())
}

