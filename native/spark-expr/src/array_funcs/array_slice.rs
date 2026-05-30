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

// Spark-compatible slice(array, start, length).
//
// Differs from datafusion-spark's SparkSlice in that we correctly return an
// empty array when a negative start position lies before the beginning of the
// array. The upstream implementation (as of datafusion-spark 53.1.0) produces
// the first element instead. Once the upstream is fixed, this can be removed
// in favour of datafusion_spark::function::array::slice::SparkSlice.

use arrow::array::{
    make_array, Array, ArrayRef, AsArray, Capacities, GenericListArray, Int64Array,
    MutableArrayData, NullBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{cast::as_int64_array, exec_err, utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraySlice {
    signature: Signature,
}

impl Default for SparkArraySlice {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraySlice {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArraySlice {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_array_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion::common::internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let row_count = args.number_rows;
        let arrays = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(a) => Ok(Arc::clone(a)),
                ColumnarValue::Scalar(s) => s.to_array_of_size(row_count),
            })
            .collect::<Result<Vec<_>>>()?;
        let [array, start, length] = take_function_args(self.name(), &arrays)?;
        let start = as_int64_array(&start)?;
        let length = as_int64_array(&length)?;

        let result = match array.data_type() {
            DataType::List(_) => slice_list::<i32>(array.as_list::<i32>(), start, length)?,
            DataType::LargeList(_) => slice_list::<i64>(array.as_list::<i64>(), start, length)?,
            other => {
                return exec_err!("{} does not support type '{other}'", self.name());
            }
        };
        Ok(ColumnarValue::Array(result))
    }
}

fn slice_list<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    start: &Int64Array,
    length: &Int64Array,
) -> Result<ArrayRef> {
    let list_field = match list_array.data_type() {
        DataType::List(field) | DataType::LargeList(field) => field,
        other => {
            return exec_err!("expected List or LargeList, got {other:?}");
        }
    };

    let values = list_array.values();
    let original_data = values.to_data();
    let row_count = list_array.len();
    let mut offsets = Vec::<O>::with_capacity(row_count + 1);
    let mut last_offset = O::zero();
    offsets.push(last_offset);
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data],
        true,
        Capacities::Array(original_data.len()),
    );
    let mut nulls = NullBufferBuilder::new(row_count);

    let row_offsets = list_array.offsets();
    let list_nulls = list_array.nulls();
    let start_nulls = start.nulls();
    let length_nulls = length.nulls();
    for row in 0..row_count {
        let is_row_null = list_nulls.is_some_and(|n| n.is_null(row))
            || start_nulls.is_some_and(|n| n.is_null(row))
            || length_nulls.is_some_and(|n| n.is_null(row));
        if is_row_null {
            offsets.push(last_offset);
            nulls.append_null();
            continue;
        }

        let start_value = start.value(row);
        let length_value = length.value(row);

        if start_value == 0 {
            return exec_err!("Unexpected value for start in function slice. Expected a positive or negative number, but got 0.");
        }
        if length_value < 0 {
            return exec_err!(
                "Unexpected value for length in function slice. Expected a non-negative number, but got {length_value}."
            );
        }

        let row_start = row_offsets[row].as_usize();
        let row_end = row_offsets[row + 1].as_usize();
        let arr_len = (row_end - row_start) as i64;

        let zero_based_start = if start_value > 0 {
            start_value - 1
        } else {
            start_value + arr_len
        };

        let copied = if zero_based_start < 0 || zero_based_start >= arr_len || length_value == 0 {
            0
        } else {
            let take = std::cmp::min(length_value, arr_len - zero_based_start) as usize;
            let begin = row_start + zero_based_start as usize;
            mutable.extend(0, begin, begin + take);
            take
        };

        last_offset += O::usize_as(copied);
        offsets.push(last_offset);
        nulls.append_non_null();
    }

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(list_field),
        OffsetBuffer::new(offsets.into()),
        make_array(mutable.freeze()),
        nulls.finish(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Int32Type};

    fn build_list(rows: Vec<Option<Vec<Option<i32>>>>) -> Arc<ListArray> {
        let mut offsets = vec![0i32];
        let mut values: Vec<Option<i32>> = Vec::new();
        let mut nulls = NullBufferBuilder::new(rows.len());
        for row in &rows {
            match row {
                Some(items) => {
                    nulls.append_non_null();
                    values.extend(items.iter().copied());
                }
                None => nulls.append_null(),
            }
            offsets.push(values.len() as i32);
        }
        let values = Arc::new(Int32Array::from(values)) as ArrayRef;
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            nulls.finish(),
        ))
    }

    fn run(
        list: Arc<ListArray>,
        start: Vec<Option<i64>>,
        length: Vec<Option<i64>>,
    ) -> Vec<Option<Vec<Option<i32>>>> {
        let start = Int64Array::from(start);
        let length = Int64Array::from(length);
        let result = slice_list::<i32>(list.as_ref(), &start, &length).unwrap();
        let result = result.as_list::<i32>();
        (0..result.len())
            .map(|i| {
                if result.is_null(i) {
                    None
                } else {
                    let row = result.value(i);
                    let row = row.as_primitive::<Int32Type>();
                    Some(
                        (0..row.len())
                            .map(|j| {
                                if row.is_null(j) {
                                    None
                                } else {
                                    Some(row.value(j))
                                }
                            })
                            .collect(),
                    )
                }
            })
            .collect()
    }

    #[test]
    fn positive_start() {
        let list = build_list(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        assert_eq!(
            run(list, vec![Some(2)], vec![Some(3)]),
            vec![Some(vec![Some(2), Some(3), Some(4)])]
        );
    }

    #[test]
    fn length_clamped_to_array_end() {
        let list = build_list(vec![Some(vec![Some(1), Some(2), Some(3)])]);
        assert_eq!(
            run(list, vec![Some(2)], vec![Some(100)]),
            vec![Some(vec![Some(2), Some(3)])]
        );
    }

    #[test]
    fn length_zero_returns_empty() {
        let list = build_list(vec![Some(vec![Some(1), Some(2), Some(3)])]);
        assert_eq!(
            run(list, vec![Some(1)], vec![Some(0)]),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn start_past_end_returns_empty() {
        let list = build_list(vec![Some(vec![Some(1), Some(2), Some(3)])]);
        assert_eq!(
            run(list, vec![Some(10)], vec![Some(1)]),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn negative_start_counts_from_end() {
        let list = build_list(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        assert_eq!(
            run(list, vec![Some(-2)], vec![Some(2)]),
            vec![Some(vec![Some(4), Some(5)])]
        );
    }

    #[test]
    fn negative_start_overflows_returns_empty() {
        // Spark: slice([a], -2, 2) returns []. datafusion-spark returns [a] here.
        let list = build_list(vec![Some(vec![Some(1)])]);
        assert_eq!(
            run(list, vec![Some(-2)], vec![Some(2)]),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn negative_start_far_below_zero_returns_empty() {
        let list = build_list(vec![Some(vec![Some(1), Some(2), Some(3)])]);
        assert_eq!(
            run(list, vec![Some(-10)], vec![Some(2)]),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn negative_start_with_length_past_end() {
        let list = build_list(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        assert_eq!(
            run(list, vec![Some(-2)], vec![Some(5)]),
            vec![Some(vec![Some(4), Some(5)])]
        );
    }

    #[test]
    fn null_inputs_yield_null() {
        let list = build_list(vec![None, Some(vec![Some(1)]), Some(vec![Some(1)])]);
        assert_eq!(
            run(
                list,
                vec![Some(1), None, Some(1)],
                vec![Some(1), Some(1), None]
            ),
            vec![None, None, None]
        );
    }

    #[test]
    fn empty_array_input() {
        let list = build_list(vec![Some(Vec::new())]);
        assert_eq!(
            run(list, vec![Some(1)], vec![Some(2)]),
            vec![Some(Vec::new())]
        );
    }

    #[test]
    fn start_zero_errors() {
        let list = build_list(vec![Some(vec![Some(1)])]);
        let start = Int64Array::from(vec![Some(0)]);
        let length = Int64Array::from(vec![Some(1)]);
        assert!(slice_list::<i32>(list.as_ref(), &start, &length).is_err());
    }

    #[test]
    fn negative_length_errors() {
        let list = build_list(vec![Some(vec![Some(1)])]);
        let start = Int64Array::from(vec![Some(1)]);
        let length = Int64Array::from(vec![Some(-1)]);
        assert!(slice_list::<i32>(list.as_ref(), &start, &length).is_err());
    }
}
