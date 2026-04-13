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

// Spark-compatible array_compact: removes null elements from an array.
//
// DataFusion's array_remove_all(arr, null) returns NULL for the entire row
// when the element-to-remove is NULL (DF 53, PR #21013). Spark's array_compact
// needs to actually remove null elements, so we implement it directly.
//
// TODO: upstream this to datafusion-spark crate

use arrow::array::{
    make_array, Array, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullBufferBuilder,
    OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayCompact {
    signature: Signature,
}

impl Default for SparkArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayCompact {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion::common::internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array] = take_function_args(self.name(), &args.args)?;
        match array {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::List(_) => Ok(ColumnarValue::Array(compact_list::<i32>(
                    array.as_any().downcast_ref().unwrap(),
                )?)),
                DataType::LargeList(_) => Ok(ColumnarValue::Array(compact_list::<i64>(
                    array.as_any().downcast_ref().unwrap(),
                )?)),
                other => exec_err!("spark_array_compact does not support type '{other}'"),
            },
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let result = match array.data_type() {
                    DataType::List(_) => {
                        compact_list::<i32>(array.as_any().downcast_ref().unwrap())?
                    }
                    DataType::LargeList(_) => {
                        compact_list::<i64>(array.as_any().downcast_ref().unwrap())?
                    }
                    other => {
                        return exec_err!("spark_array_compact does not support type '{other}'")
                    }
                };
                Ok(ColumnarValue::Array(result))
            }
        }
    }
}

/// Remove null elements from each row of a list array.
fn compact_list<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
) -> Result<ArrayRef> {
    let list_field = match list_array.data_type() {
        DataType::List(field) | DataType::LargeList(field) => field,
        other => {
            return exec_err!("Expected List or LargeList, got {other:?}");
        }
    };

    let values = list_array.values();
    let original_data = values.to_data();
    let mut offsets = Vec::<OffsetSize>::with_capacity(list_array.len() + 1);
    offsets.push(OffsetSize::zero());
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data],
        false,
        Capacities::Array(original_data.len()),
    );
    let mut valid = NullBufferBuilder::new(list_array.len());

    // Use logical_nulls() instead of is_null() to correctly handle NullArray.
    // NullArray::nulls() returns None (which makes is_null() return false),
    // but logical_nulls() correctly reports all elements as null.
    let value_nulls = values.logical_nulls();

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row_index) {
            offsets.push(offsets[row_index]);
            valid.append_null();
            continue;
        }

        let start = offset_window[0].to_usize().unwrap();
        let end = offset_window[1].to_usize().unwrap();
        let mut copied = 0usize;

        for i in start..end {
            let is_null = value_nulls
                .as_ref()
                .map(|n| n.is_null(i))
                .unwrap_or(false);
            if !is_null {
                mutable.extend(0, i, i + 1);
                copied += 1;
            }
        }

        offsets.push(offsets[row_index] + OffsetSize::usize_as(copied));
        valid.append_non_null();
    }

    let new_values = make_array(mutable.freeze());
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(list_field),
        OffsetBuffer::new(offsets.into()),
        new_values,
        valid.finish(),
    )?))
}
