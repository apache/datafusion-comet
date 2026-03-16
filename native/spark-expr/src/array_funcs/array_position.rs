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
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, Int64Array, OffsetSizeTrait,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, TimestampMicrosecondType,
};
use datafusion::common::{exec_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark array_position() function that returns the 1-based position of an element in an array.
/// Returns 0 if the element is not found (Spark behavior differs from DataFusion which returns null).
fn spark_array_position(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 2 {
        return exec_err!("array_position function takes exactly two arguments");
    }

    // Convert all arguments to arrays for consistent processing
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();
    let arrays = ColumnarValue::values_to_arrays(args)?;

    let result = array_position_inner(&arrays)?;

    if is_scalar {
        let scalar = ScalarValue::try_from_array(&result, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

fn array_position_inner(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let array = &args[0];
    let element = &args[1];

    match array.data_type() {
        DataType::List(_) => generic_array_position::<i32>(array, element),
        DataType::LargeList(_) => generic_array_position::<i64>(array, element),
        other => exec_err!("array_position does not support type '{other:?}'"),
    }
}

/// Find the 1-based position of `search_val` in a typed primitive array.
/// Returns 0 if not found.
macro_rules! find_position_primitive {
    ($list_items:expr, $element:expr, $row_index:expr, $arrow_type:ty) => {{
        let items = $list_items.as_primitive::<$arrow_type>();
        let search = $element.as_primitive::<$arrow_type>();
        let search_val = search.value($row_index);
        let mut pos: i64 = 0;
        for i in 0..items.len() {
            if !items.is_null(i) && items.value(i) == search_val {
                pos = (i + 1) as i64;
                break;
            }
        }
        pos
    }};
}

fn find_position_in_row(
    list_items: &ArrayRef,
    element: &ArrayRef,
    row_index: usize,
) -> Result<i64, DataFusionError> {
    let pos = match list_items.data_type() {
        DataType::Boolean => {
            let items = list_items.as_any().downcast_ref::<BooleanArray>().unwrap();
            let search = element.as_any().downcast_ref::<BooleanArray>().unwrap();
            let search_val = search.value(row_index);
            let mut pos: i64 = 0;
            for i in 0..items.len() {
                if !items.is_null(i) && items.value(i) == search_val {
                    pos = (i + 1) as i64;
                    break;
                }
            }
            pos
        }
        DataType::Int8 => find_position_primitive!(list_items, element, row_index, Int8Type),
        DataType::Int16 => find_position_primitive!(list_items, element, row_index, Int16Type),
        DataType::Int32 => find_position_primitive!(list_items, element, row_index, Int32Type),
        DataType::Int64 => find_position_primitive!(list_items, element, row_index, Int64Type),
        DataType::Float32 => {
            find_position_primitive!(list_items, element, row_index, Float32Type)
        }
        DataType::Float64 => {
            find_position_primitive!(list_items, element, row_index, Float64Type)
        }
        DataType::Decimal128(_, _) => {
            find_position_primitive!(list_items, element, row_index, Decimal128Type)
        }
        DataType::Date32 => {
            find_position_primitive!(list_items, element, row_index, Date32Type)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            find_position_primitive!(list_items, element, row_index, TimestampMicrosecondType)
        }
        DataType::Utf8 => {
            let items = list_items.as_string::<i32>();
            let search = element.as_string::<i32>();
            let search_val = search.value(row_index);
            let mut pos: i64 = 0;
            for i in 0..items.len() {
                if !items.is_null(i) && items.value(i) == search_val {
                    pos = (i + 1) as i64;
                    break;
                }
            }
            pos
        }
        DataType::LargeUtf8 => {
            let items = list_items.as_string::<i64>();
            let search = element.as_string::<i64>();
            let search_val = search.value(row_index);
            let mut pos: i64 = 0;
            for i in 0..items.len() {
                if !items.is_null(i) && items.value(i) == search_val {
                    pos = (i + 1) as i64;
                    break;
                }
            }
            pos
        }
        // Fallback to ScalarValue for complex types (nested arrays, etc.)
        _ => {
            let element_scalar = ScalarValue::try_from_array(element, row_index)?;
            let mut pos: i64 = 0;
            for i in 0..list_items.len() {
                let item_scalar = ScalarValue::try_from_array(list_items, i)?;
                if !item_scalar.is_null() && element_scalar == item_scalar {
                    pos = (i + 1) as i64;
                    break;
                }
            }
            pos
        }
    };
    Ok(pos)
}

fn generic_array_position<O: OffsetSizeTrait>(
    array: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let list_array = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .unwrap();

    let mut data = Vec::with_capacity(list_array.len());

    for row_index in 0..list_array.len() {
        if list_array.is_null(row_index) || element.is_null(row_index) {
            data.push(None);
        } else {
            let list_array_row = list_array.value(row_index);
            let position = find_position_in_row(&list_array_row, element, row_index)?;
            data.push(Some(position));
        }
    }

    Ok(Arc::new(Int64Array::from(data)))
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayPositionFunc {
    signature: Signature,
}

impl Default for SparkArrayPositionFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayPositionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayPositionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        spark_array_position(&args.args)
    }
}
