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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_physical_expr::PhysicalExpr;

use crate::execution::datafusion::expressions::utils::down_cast_any_ref;

#[derive(Debug, Hash)]
pub struct CreateNamedStruct {
    values: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
}

impl CreateNamedStruct {
    pub fn new(values: Vec<Arc<dyn PhysicalExpr>>, data_type: DataType) -> Self {
        Self { values, data_type }
    }
}

impl PhysicalExpr for CreateNamedStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let values = self
            .values
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let arrays = ColumnarValue::values_to_arrays(&values)?;
        let fields = match &self.data_type {
            DataType::Struct(fields) => fields,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Expected struct data type, got {:?}",
                    self.data_type
                )))
            }
        };
        Ok(ColumnarValue::Array(Arc::new(StructArray::new(
            fields.clone(),
            arrays,
            None,
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.values.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CreateNamedStruct::new(
            children.clone(),
            self.data_type.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.values.hash(&mut s);
        self.data_type.hash(&mut s);
        self.hash(&mut s);
    }
}

impl Display for CreateNamedStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateNamedStruct [values: {:?}, data_type: {:?}]",
            self.values, self.data_type
        )
    }
}

impl PartialEq<dyn Any> for CreateNamedStruct {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.values
                    .iter()
                    .zip(x.values.iter())
                    .all(|(a, b)| a.eq(b))
                    && self.data_type.eq(&x.data_type)
            })
            .unwrap_or(false)
    }
}
