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

use crate::{errors::CometError, execution::datafusion::expressions::utils::down_cast_any_ref};
use arrow_array::{builder::BooleanBuilder, Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use regex::Regex;
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
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.pattern.eq(&x.pattern))
            .unwrap_or(false)
    }
}

impl PhysicalExpr for RLike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion_common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        if let ColumnarValue::Array(v) = self.child.evaluate(batch)? {
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) =
                self.pattern.evaluate(batch)?
            {
                // TODO cache Regex across invocations of evaluate() or create it in constructor
                match Regex::new(&pattern) {
                    Ok(re) => {
                        let inputs = v
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("string array");
                        let mut builder = BooleanBuilder::with_capacity(inputs.len());
                        if inputs.is_nullable() {
                            for i in 0..inputs.len() {
                                if inputs.is_null(i) {
                                    builder.append_null();
                                } else {
                                    builder.append_value(re.is_match(inputs.value(i)));
                                }
                            }
                        } else {
                            for i in 0..inputs.len() {
                                builder.append_value(re.is_match(inputs.value(i)));
                            }
                        }
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    Err(e) => Err(CometError::Internal(format!(
                        "Failed to compile regular expression: {e:?}"
                    ))
                    .into()),
                }
            } else {
                Err(
                    CometError::Internal("Only scalar regex patterns are supported".to_string())
                        .into(),
                )
            }
        } else {
            Err(CometError::Internal("Only columnar inputs are supported".to_string()).into())
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 2);
        Ok(Arc::new(RLike::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        use std::hash::Hash;
        let mut s = state;
        self.hash(&mut s);
    }
}
