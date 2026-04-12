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
    as_large_list_array, as_list_array, Array, ArrayRef, GenericListArray, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{Row, RowConverter, SortField};
use datafusion::common::utils::take_function_args;
use datafusion::common::Result;
use datafusion::common::{exec_err, HashSet};
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayDistinct {
    signature: Signature,
}

impl Default for SparkArrayDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayDistinct {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    List(Arc::new(Field::new("item", Null, true))),
                    LargeList(Arc::new(Field::new("item", Null, true))),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkArrayDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_distinct_inner, vec![])(&args.args)
    }
}

fn array_distinct_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_distinct", args)?;
    match array.data_type() {
        DataType::List(field) => {
            let array = as_list_array(array);
            general_array_distinct(array, field)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(array);
            general_array_distinct(array, field)
        }
        _ => {
            exec_err!(
                "array_distinct function only support arrays, got: {:?}",
                array.data_type()
            )
        }
    }
}

fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    if array.is_empty() {
        return Ok(Arc::new(array.clone()) as ArrayRef);
    }

    let value_offsets = array.value_offsets();
    let original_data = array.values().to_data();
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    let converter = RowConverter::new(vec![SortField::new(dt.clone())])?;

    let first_offset = value_offsets[0].as_usize();
    let visible_len = value_offsets[array.len()].as_usize() - first_offset;
    let rows = converter.convert_columns(&[array.values().slice(first_offset, visible_len)])?;

    let mut mutable = arrow::array::MutableArrayData::new(vec![&original_data], false, visible_len);

    for i in 0..array.len() {
        let last_offset = *offsets.last().unwrap();

        if array.is_null(i) {
            offsets.push(last_offset);
            continue;
        }

        let start = value_offsets[i].as_usize() - first_offset;
        let end = value_offsets[i + 1].as_usize() - first_offset;
        let array_len = end - start;

        let mut seen: HashSet<Row<'_>> = HashSet::with_capacity(array_len);
        let mut seen_null = false;
        let mut distinct_count: usize = 0;

        for idx in start..end {
            let abs_idx = idx + first_offset;

            if array.values().is_null(abs_idx) {
                if !seen_null {
                    seen_null = true;
                    mutable.extend(0, abs_idx, abs_idx + 1);
                    distinct_count += 1;
                }
            } else {
                let row = rows.row(idx);
                if seen.insert(row) {
                    mutable.extend(0, abs_idx, abs_idx + 1);
                    distinct_count += 1;
                }
            }
        }

        offsets.push(last_offset + OffsetSize::usize_as(distinct_count));
    }

    let final_values = arrow::array::make_array(mutable.freeze());

    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        final_values,
        array.nulls().cloned(),
    )?))
}
