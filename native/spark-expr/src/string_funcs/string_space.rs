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

#![allow(deprecated)]

use crate::kernels::strings::string_space;
use arrow::record_batch::RecordBatch;
use arrow_array::cast::as_dictionary_array;
use arrow_array::types::Int32Type;
use arrow_array::{
    make_array, Array, ArrayRef, DictionaryArray, GenericStringArray, Int32Array, OffsetSizeTrait,
};
use arrow_buffer::MutableBuffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct StringSpaceExpr {
    pub child: Arc<dyn PhysicalExpr>,
}

impl Hash for StringSpaceExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PartialEq for StringSpaceExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl StringSpaceExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl Display for StringSpaceExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringSpace [child: {}] ", self.child)
    }
}

impl PhysicalExpr for StringSpaceExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema)? {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Utf8)))
            }
            _ => Ok(DataType::Utf8),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result = string_space_kernel(&array)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "StringSpace(scalar) should be fold in Spark JVM side.".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StringSpaceExpr::new(Arc::clone(&children[0]))))
    }
}

/// Returns an ArrayRef with a string consisting of `length` spaces.
///
/// # Preconditions
///
/// - elements in `length` must not be negative
pub fn string_space_kernel(length: &dyn Array) -> Result<ArrayRef, DataFusionError> {
    match length.data_type() {
        DataType::Int32 => {
            let array = length.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(generic_string_space::<i32>(array))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(length);
            let values = string_space_kernel(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!(
            "Unsupported input type for function 'string_space': {:?}",
            dt
        ),
    }
}

fn generic_string_space<OffsetSize: OffsetSizeTrait>(length: &Int32Array) -> ArrayRef {
    let array_len = length.len();
    let mut offsets = MutableBuffer::new((array_len + 1) * std::mem::size_of::<OffsetSize>());
    let mut length_so_far = OffsetSize::zero();

    // compute null bitmap (copy)
    let null_bit_buffer = length.to_data().nulls().map(|b| b.buffer().clone());

    // Gets slice of length array to access it directly for performance.
    let length_data = length.to_data();
    let lengths = length_data.buffers()[0].typed_data::<i32>();
    let total = lengths.iter().map(|l| *l as usize).sum::<usize>();
    let mut values = MutableBuffer::new(total);

    offsets.push(length_so_far);

    let blank = " ".as_bytes()[0];
    values.resize(total, blank);

    (0..array_len).for_each(|i| {
        let current_len = lengths[i] as usize;

        length_so_far += OffsetSize::from_usize(current_len).unwrap();
        offsets.push(length_so_far);
    });

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array_len,
            None,
            null_bit_buffer,
            0,
            vec![offsets.into(), values.into()],
            vec![],
        )
    };
    make_array(data)
}
