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

use std::sync::Arc;

use arrow::array::{make_array, ArrayData, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, FieldRef, Schema, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
};
use datafusion::common::Result;

fn millis_type(data_type: &DataType) -> DataType {
    let field = |f: &FieldRef| {
        Arc::new(
            f.as_ref()
                .clone()
                .with_data_type(millis_type(f.data_type())),
        )
    };
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::clone(tz)))
        }
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::FixedSizeList(f, size) => DataType::FixedSizeList(field(f), *size),
        DataType::Map(f, ordered) => DataType::Map(field(f), *ordered),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(millis_type(value)))
        }
        other => other.clone(),
    }
}

pub(crate) fn millis_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| {
                f.as_ref()
                    .clone()
                    .with_data_type(millis_type(f.data_type()))
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

pub(crate) fn conversion_bytes(array: &ArrayData) -> usize {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => array.len() * size_of::<i64>(),
        _ => array.child_data().iter().map(conversion_bytes).sum(),
    }
}

pub(crate) fn to_millis(array: &ArrayRef) -> Result<ArrayRef> {
    let data_type = millis_type(array.data_type());
    if &data_type == array.data_type() {
        return Ok(Arc::clone(array));
    }
    if let DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) = &data_type {
        // Spark's microsToMillis uses floor division, including for negative epoch values.
        let values = array
            .as_primitive::<TimestampMicrosecondType>()
            .unary::<_, TimestampMillisecondType>(|v| v.div_euclid(1000))
            .with_timezone(Arc::clone(tz));
        return Ok(Arc::new(values));
    }
    let data = array.to_data();
    let children = data
        .child_data()
        .iter()
        .map(|child| to_millis(&make_array(child.clone())).map(|a| a.to_data()))
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(data_type)
            .child_data(children)
            .build()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray, StructArray, TimestampMicrosecondArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    #[test]
    fn millis_rounds_down_and_preserves_ntz_and_untouched_buffers() {
        let values: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(-1001), Some(-1), None, Some(123456)])
                .with_timezone("UTC"),
        );
        let converted = to_millis(&values).unwrap();
        assert_eq!(
            converted
                .as_primitive::<TimestampMillisecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-2), Some(-1), None, Some(123)]
        );
        let ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![-1]));
        assert!(Arc::ptr_eq(&ntz, &to_millis(&ntz).unwrap()));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let fields = vec![
            Arc::new(Field::new("ts", values.data_type().clone(), true)),
            Arc::new(Field::new("id", DataType::Int32, false)),
        ];
        let array: ArrayRef = Arc::new(
            StructArray::new(fields.into(), vec![values, Arc::clone(&ints)], None).slice(1, 3),
        );
        let converted = to_millis(&array).unwrap();
        assert_eq!(
            converted
                .as_struct()
                .column(0)
                .as_primitive::<TimestampMillisecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-1), None, Some(123)]
        );
        assert_eq!(
            array.as_struct().column(1).to_data().buffers(),
            converted.as_struct().column(1).to_data().buffers()
        );
    }

    #[test]
    fn nested_conversion_preserves_sliced_offsets_and_parent_nulls() {
        let values: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![-1001, -1, 123456]).with_timezone("UTC"));
        let list = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            values,
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let input: ArrayRef = Arc::new(list.slice(1, 2));
        let output = to_millis(&input).unwrap();
        assert_eq!(output.as_list::<i32>().value_offsets(), &[1, 2, 3]);
        assert_eq!(
            output
                .as_list::<i32>()
                .value(0)
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            -1
        );
        assert_eq!(
            output
                .as_list::<i32>()
                .value(1)
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            123
        );
        assert_eq!(output.null_count(), input.null_count());
        for i in 0..input.len() {
            assert_eq!(output.is_null(i), input.is_null(i));
        }
        let input: ArrayRef = Arc::new(list.slice(0, 2));
        let output = to_millis(&input).unwrap();
        assert!(output.is_null(0));
        assert!(output.is_valid(1));
    }
}
