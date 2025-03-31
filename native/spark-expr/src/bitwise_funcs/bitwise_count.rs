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
use std::hash::Hash;
use std::sync::Arc;
use arrow::{
    array::*,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::Result;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

macro_rules! check_overflow {
    ($VALUE:expr, $TYPE:expr, $TYPE_NAME:expr) => {{
        if $VALUE == $TYPE::MIN {
            if $TYPE_NAME == "byte" || $TYPE_NAME == "short" {
                let msg = format!("{:?} caused", $VALUE);
                return Err(arithmetic_overflow_error(msg.as_str()).into());
            }
            return Err(arithmetic_overflow_error($TYPE_NAME).into());
        }
    }};
}

macro_rules! compute_op {
    ($OPERAND:expr, $DT:ident, $TYPE:expr, $TYPE_NAME:expr) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");

        let result: $DT = operand
            .iter()
            .map(|x| { x.map(|y| { check_overflow!(y.count_ones(), $TYPE, $TYPE_NAME) as $TYPE })})
            .collect();

        Ok(Arc::new(result))
    }};
}

/// BitwiseCount expression
#[derive(Debug, Eq)]
pub struct BitwiseCountExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl Hash for BitwiseCountExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl PartialEq for BitwiseCountExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl BitwiseCountExpr {
    /// Create new bitwise count expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for BitwiseCountExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(~ {})", self.arg)
    }
}

impl PhysicalExpr for BitwiseCountExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result: Result<ArrayRef> = match array.data_type() {
                    DataType::Int8 => compute_op!(array, Int8Array, i8, "byte"),
                    DataType::Int16 => compute_op!(array, Int16Array, i16, "short"),
                    DataType::Int32 => compute_op!(array, Int32Array, i32, "integer"),
                    DataType::Int64 => compute_op!(array, Int64Array, i64, "long"),
                    _ => Err(DataFusionError::Execution(format!(
                        "(- '{:?}') can't be evaluated because the expression's type is {:?}, not signed int",
                        self,
                        array.data_type(),
                    ))),
                };
                result.map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                "shouldn't go to bitwise not scalar path".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(BitwiseCountExpr::new(Arc::clone(&children[0]))))
    }
}

pub fn bitwise_count(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(BitwiseCountExpr::new(arg)))
}

#[cfg(test)]
mod tests {

    #[test]
    fn bitwise_count_op() -> datafusion::common::Result<()> {
        Ok(())
    }
}