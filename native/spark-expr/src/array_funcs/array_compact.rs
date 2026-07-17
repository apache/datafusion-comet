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

    // Fast path: array_compact only removes null elements. When the values buffer has no
    // logical nulls there is nothing to remove, so every row is returned unchanged and the
    // result is bit-identical to the input (same offsets, values, and row null buffer).
    // logical_null_count() (not null_count) is used so a NullArray, whose elements are all
    // logically null, is counted correctly; NullArray::nulls() is None. It is also
    // allocation-free for the Dictionary/Run/Null overrides, while logical_nulls() would
    // materialize a fresh bitmap on exactly this path.
    if values.logical_null_count() == 0 {
        return Ok(Arc::new(list_array.clone()));
    }

    // logical_nulls() (not nulls()) is used below so a NullArray values child, whose
    // elements are all logically null, is reported correctly. NullArray::nulls() returns
    // None, which would make is_null() report false for every element.
    let value_nulls = values.logical_nulls();

    let original_data = values.to_data();
    let mut offsets = Vec::<OffsetSize>::with_capacity(list_array.len() + 1);
    offsets.push(OffsetSize::zero());
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data],
        false,
        Capacities::Array(original_data.len()),
    );
    let mut valid = NullBufferBuilder::new(list_array.len());

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
            let is_null = value_nulls.as_ref().map(|n| n.is_null(i)).unwrap_or(false);
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int32Builder, ListArray, ListBuilder};

    /// Build a `ListArray<Int32>`. `None` row = null row; inner `None` = null element.
    fn i32_list(rows: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
        let mut b = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                None => b.append(false),
                Some(elems) => {
                    for e in elems {
                        b.values().append_option(e);
                    }
                    b.append(true);
                }
            }
        }
        b.finish()
    }

    fn read(arr: &ArrayRef) -> Vec<Option<Vec<Option<i32>>>> {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        (0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    let v = list.value(i);
                    let v = v.as_any().downcast_ref::<Int32Array>().unwrap();
                    Some(
                        (0..v.len())
                            .map(|j| (!v.is_null(j)).then(|| v.value(j)))
                            .collect(),
                    )
                }
            })
            .collect()
    }

    #[test]
    fn no_element_nulls_returns_input_bit_identical() {
        // Includes a null row and an empty row: the fast path must preserve both.
        let input = i32_list(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![]),
            Some(vec![Some(3)]),
        ]);
        let out = compact_list::<i32>(&input).unwrap();
        // Fast path returns the input unchanged, so ArrayData must be identical.
        assert_eq!(input.to_data(), out.to_data());
    }

    #[test]
    fn removes_null_elements_preserving_rows() {
        let input = i32_list(vec![
            Some(vec![Some(1), None, Some(2)]),
            Some(vec![None, None]),
            None,
            Some(vec![Some(3)]),
        ]);
        let out = compact_list::<i32>(&input).unwrap();
        assert_eq!(
            read(&out),
            vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![]),
                None,
                Some(vec![Some(3)]),
            ]
        );
    }

    #[test]
    fn all_null_elements_become_empty_rows() {
        let input = i32_list(vec![Some(vec![None, None]), Some(vec![None])]);
        let out = compact_list::<i32>(&input).unwrap();
        assert_eq!(read(&out), vec![Some(vec![]), Some(vec![])]);
    }

    #[test]
    fn null_array_values_child_all_rows_empty() {
        // DataFusion's make_array(NULL) produces a List<Null> whose values child is a
        // NullArray. NullArray::nulls() is None yet every element is logically null, so
        // this path must not take the "no logical nulls" fast branch and must compact
        // every row to empty.
        use arrow::array::NullArray;

        let null_values = Arc::new(NullArray::new(3)) as ArrayRef;
        let null_field = Arc::new(arrow::datatypes::Field::new("item", DataType::Null, true));
        let input = GenericListArray::<i32>::new(
            null_field,
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            null_values,
            None,
        );
        let out = compact_list::<i32>(&input).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        for i in 0..3 {
            assert!(!list.is_null(i));
            assert_eq!(list.value_length(i), 0);
        }
    }
}
