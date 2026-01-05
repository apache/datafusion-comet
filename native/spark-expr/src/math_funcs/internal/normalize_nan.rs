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

use arrow::compute::unary;
use arrow::datatypes::{DataType, Schema};
use arrow::{
    array::{as_primitive_array, Float32Array, Float64Array},
    datatypes::{Float32Type, Float64Type},
    record_batch::RecordBatch,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct NormalizeNaNAndZero {
    pub data_type: DataType,
    pub child: Arc<dyn PhysicalExpr>,
}

impl PartialEq for NormalizeNaNAndZero {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.data_type.eq(&other.data_type)
    }
}

impl Hash for NormalizeNaNAndZero {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
    }
}

impl NormalizeNaNAndZero {
    pub fn new(data_type: DataType, child: Arc<dyn PhysicalExpr>) -> Self {
        Self { data_type, child }
    }
}

impl PhysicalExpr for NormalizeNaNAndZero {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let cv = self.child.evaluate(batch)?;
        let array = cv.into_array(batch.num_rows())?;

        match &self.data_type {
            DataType::Float32 => {
                let input = as_primitive_array::<Float32Type>(&array);
                // Use unary which operates directly on values buffer without intermediate allocation
                let result: Float32Array = unary(input, normalize_float);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let input = as_primitive_array::<Float64Type>(&array);
                // Use unary which operates directly on values buffer without intermediate allocation
                let result: Float64Array = unary(input, normalize_float);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            dt => panic!("Unexpected data type {dt:?}"),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NormalizeNaNAndZero::new(
            self.data_type.clone(),
            Arc::clone(&children[0]),
        )))
    }
}

/// Normalize a floating point value by converting all NaN representations to a canonical NaN
/// and negative zero to positive zero. This is used for Spark's comparison semantics.
#[inline]
fn normalize_float<T: num::Float>(v: T) -> T {
    if v.is_nan() {
        T::nan()
    } else if v == T::neg_zero() {
        T::zero()
    } else {
        v
    }
}

impl Display for NormalizeNaNAndZero {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FloatNormalize [child: {}]", self.child)
    }
}
