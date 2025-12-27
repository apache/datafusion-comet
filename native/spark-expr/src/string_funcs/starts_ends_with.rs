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

use arrow::array::{Array, BooleanArray, Scalar, StringArray};
use arrow::buffer::BooleanBuffer;
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
pub struct StartsWithExpr {
    pub child: Arc<dyn PhysicalExpr>,
    pub pattern_array: Arc<StringArray>, // Pre-allocated pattern
}

impl StartsWithExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, pattern: String) -> Self {
        // Optimization: Allocate the pattern array ONCE during construction
        // This avoids creating a new StringArray for every single batch
        let pattern_array = Arc::new(StringArray::from(vec![pattern]));
        Self {
            child,
            pattern_array,
        }
    }
}

impl Hash for StartsWithExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.pattern_array.value(0).hash(state);
    }
}

impl PartialEq for StartsWithExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.pattern_array.value(0) == other.pattern_array.value(0)
    }
}

impl Eq for StartsWithExpr {}

impl Display for StartsWithExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "startsWith({}, \"{}\")",
            self.child,
            self.pattern_array.value(0)
        )
    }
}

impl PhysicalExpr for StartsWithExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, _input_schema: &arrow::datatypes::Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &arrow::record_batch::RecordBatch) -> Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;

        match arg {
            ColumnarValue::Array(array) => {
                // Zero-Allocation here: We reuse the pre-allocated pattern_array
                let scalar = Scalar::new(self.pattern_array.as_ref());

                // Use Arrow's highly optimized SIMD kernel
                let result = compute::starts_with(&array, &scalar)?;

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(str_val))) => {
                // Fallback for scalar inputs (rare in big data, but necessary)
                let pattern_scalar = self.pattern_array.value(0);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    str_val.starts_with(pattern_scalar),
                ))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            _ => Err(datafusion::error::DataFusionError::Internal(
                "StartsWith requires StringArray input".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StartsWithExpr::new(
            Arc::clone(&children[0]),
            self.pattern_array.value(0).to_string(),
        )))
    }
}

// ----------------------------------------------------------------------------
// ENDS WITH IMPLEMENTATION
// ----------------------------------------------------------------------------

#[derive(Debug)]
pub struct EndsWithExpr {
    pub child: Arc<dyn PhysicalExpr>,
    pub pattern: String,    // Keep pattern as String for raw byte access
    pub pattern_len: usize, // Pre-calculate length
}

impl EndsWithExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, pattern: String) -> Self {
        let pattern_len = pattern.len();
        Self {
            child,
            pattern,
            pattern_len,
        }
    }
}

impl Hash for EndsWithExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.pattern.hash(state);
    }
}

impl PartialEq for EndsWithExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.pattern == other.pattern
    }
}

impl Eq for EndsWithExpr {}

impl Display for EndsWithExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "endsWith({}, \"{}\")", self.child, self.pattern)
    }
}

impl PhysicalExpr for EndsWithExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, _input_schema: &arrow::datatypes::Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &arrow::record_batch::RecordBatch) -> Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;

        match arg {
            ColumnarValue::Array(array) => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let len = string_array.len();

                let offsets = string_array.value_offsets();
                let values = string_array.value_data();
                let pattern_bytes = self.pattern.as_bytes();
                let p_len = self.pattern_len;

                let mut buffer = Vec::with_capacity(len.div_ceil(8));
                let mut current_byte: u8 = 0;
                let mut bit_mask: u8 = 1;

                for i in 0..len {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let str_len = end - start;

                    let is_match = if str_len >= p_len {
                        let tail_start = end - p_len;
                        &values[tail_start..end] == pattern_bytes
                    } else {
                        false
                    };

                    if is_match {
                        current_byte |= bit_mask;
                    }

                    bit_mask = bit_mask.rotate_left(1);
                    if bit_mask == 1 {
                        buffer.push(current_byte);
                        current_byte = 0;
                    }
                }

                if bit_mask != 1 {
                    buffer.push(current_byte);
                }

                let nulls = string_array.nulls().cloned();
                let boolean_buffer = BooleanBuffer::new(buffer.into(), 0, len);
                let result_array = BooleanArray::new(boolean_buffer, nulls);

                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(str_val))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(str_val.ends_with(&self.pattern))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            _ => Err(datafusion::error::DataFusionError::Internal(
                "EndsWith requires StringArray input".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(EndsWithExpr::new(
            Arc::clone(&children[0]),
            self.pattern.clone(),
        )))
    }
}
