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

use arrow::{
    array::*,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::{error::DataFusionError, logical_expr::ColumnarValue};
use std::hash::Hash;
use std::{any::Any, sync::Arc};

macro_rules! compute_op {
    ($OPERAND:expr, $DT:ident, $TY:ty) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");

        let result: $DT = operand
            .iter()
            .map(|x| { x.map(|y| { bit_count(y.into()) as $TY })})
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

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
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
                    DataType::Int8 | DataType::Boolean => compute_op!(array, Int8Array, i8),
                    DataType::Int16 => compute_op!(array, Int16Array, i16),
                    DataType::Int32 => compute_op!(array, Int32Array, i32),
                    DataType::Int64 => compute_op!(array, Int64Array, i64),
                    _ => Err(DataFusionError::Execution(format!(
                        "(- '{:?}') can't be evaluated because the expression's type is {:?}, not signed int",
                        self,
                        array.data_type(),
                    ))),
                };
                result.map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                "shouldn't go to bitwise count scalar path".to_string(),
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

// Here’s the equivalent Rust implementation of the bitCount function (similar to Apache Spark's bitCount for LongType)
fn bit_count(i: i64) -> i32 {
    let mut u = i as u64;
    u = u - ((u >> 1) & 0x5555555555555555);
    u = (u & 0x3333333333333333) + ((u >> 2) & 0x3333333333333333);
    u = (u + (u >> 4)) & 0x0f0f0f0f0f0f0f0f;
    u = u + (u >> 8);
    u = u + (u >> 16);
    u = u + (u >> 32);
    (u as i32) & 0x7f
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::*;
    use datafusion::common::{cast::as_int32_array, Result};
    use datafusion::physical_expr::expressions::col;

    use super::*;

    #[test]
    fn bitwise_count_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("field", DataType::Int32, true)]);

        let expr = bitwise_count(col("field", &schema)?)?;

        let input = Int32Array::from(vec![
            Some(1),
            None,
            Some(12345),
            Some(89),
            Some(-3456),
        ]);

        let expected = &Int32Array::from(vec![
            Some(1),
            None,
            Some(6),
            Some(4),
            Some(54),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int32_array(&result).expect("failed to downcast to In32Array");
        assert_eq!(result, expected);

        Ok(())
    }
}