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

use arrow::array::{
    as_dictionary_array, make_array, Array, ArrayData, ArrayRef, DictionaryArray,
    GenericStringArray, Int32Array, OffsetSizeTrait,
};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{DataType, Int32Type};
use datafusion::common::{exec_err, internal_datafusion_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, sync::Arc};

#[derive(Debug)]
pub struct SparkStringSpace {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkStringSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStringSpace {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkStringSpace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_space"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Dictionary(key_type, _) => {
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8))
            }
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 1] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("string_space expects exactly one argument"))?;
        spark_string_space(&args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn spark_string_space(args: &[ColumnarValue; 1]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Array(array)] => {
            let result = string_space(&array)?;

            Ok(ColumnarValue::Array(result))
        }
        _ => exec_err!("StringSpace(scalar) should be fold in Spark JVM side."),
    }
}

fn string_space(length: &dyn Array) -> std::result::Result<ArrayRef, DataFusionError> {
    match length.data_type() {
        DataType::Int32 => {
            let array = length.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(generic_string_space::<i32>(array))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(length);
            let values = string_space(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        other => exec_err!("Unsupported input type for function 'string_space': {other:?}"),
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
