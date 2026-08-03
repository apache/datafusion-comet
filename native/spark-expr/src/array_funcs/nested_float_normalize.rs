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
    Array, ArrayRef, AsArray, FixedSizeListArray, Float32Array, Float64Array, LargeListArray,
    ListArray, StructArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Recursively rebuilds nested arrays with `-0.0` normalized to `0.0` in any
/// Float32/Float64 leaves, leaving NaN untouched.
pub(super) fn normalize_negative_zero(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
            let normalized: Float32Array = arr
                .iter()
                .map(|v| v.map(|v| if v == 0.0 { 0.0f32 } else { v }))
                .collect();
            Arc::new(normalized)
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
            let normalized: Float64Array = arr
                .iter()
                .map(|v| v.map(|v| if v == 0.0 { 0.0f64 } else { v }))
                .collect();
            Arc::new(normalized)
        }
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let normalized_values = normalize_negative_zero(list.values());
            Arc::new(ListArray::new(
                Arc::clone(field),
                list.offsets().clone(),
                normalized_values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            let normalized_values = normalize_negative_zero(list.values());
            Arc::new(LargeListArray::new(
                Arc::clone(field),
                list.offsets().clone(),
                normalized_values,
                list.nulls().cloned(),
            ))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let normalized_values = normalize_negative_zero(list.values());
            Arc::new(FixedSizeListArray::new(
                Arc::clone(field),
                *size,
                normalized_values,
                list.nulls().cloned(),
            ))
        }
        DataType::Struct(_) => {
            let s = array.as_struct();
            let normalized_columns: Vec<ArrayRef> =
                s.columns().iter().map(normalize_negative_zero).collect();
            Arc::new(StructArray::new(
                s.fields().clone(),
                normalized_columns,
                s.nulls().cloned(),
            ))
        }
        _ => Arc::clone(array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Builder;
    use arrow::array::ListBuilder;
    use arrow::datatypes::Field;

    #[test]
    fn test_normalize_flat_floats() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            None,
            Some(1.5),
        ]));
        let normalized = normalize_negative_zero(&arr);
        let normalized = normalized.as_primitive::<arrow::datatypes::Float64Type>();

        assert_eq!(normalized.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(normalized.value(1).to_bits(), 0.0f64.to_bits());
        assert!(normalized.value(2).is_nan());
        assert!(normalized.is_null(3));
        assert_eq!(normalized.value(4), 1.5);
    }

    #[test]
    fn test_normalize_nested_list_floats() {
        let mut builder = ListBuilder::new(Float64Builder::new());
        builder.values().append_value(-0.0);
        builder.values().append_value(f64::NAN);
        builder.append(true);
        let arr: ArrayRef = Arc::new(builder.finish());

        let normalized = normalize_negative_zero(&arr);
        let normalized = normalized.as_list::<i32>();
        let inner = normalized.value(0);
        let inner = inner.as_primitive::<arrow::datatypes::Float64Type>();

        assert_eq!(inner.value(0).to_bits(), 0.0f64.to_bits());
        assert!(inner.value(1).is_nan());
    }

    #[test]
    fn test_normalize_struct_floats() {
        let a = Float64Array::from(vec![Some(-0.0), Some(1.0)]);
        let b = Float64Array::from(vec![Some(f64::NAN), Some(-0.0)]);
        let fields = vec![
            Arc::new(Field::new("a", DataType::Float64, true)),
            Arc::new(Field::new("b", DataType::Float64, true)),
        ];
        let arr: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![Arc::new(a), Arc::new(b)],
            None,
        ));

        let normalized = normalize_negative_zero(&arr);
        let normalized = normalized.as_struct();
        let col_a = normalized
            .column(0)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let col_b = normalized
            .column(1)
            .as_primitive::<arrow::datatypes::Float64Type>();

        assert_eq!(col_a.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(col_a.value(1), 1.0);
        assert!(col_b.value(0).is_nan());
        assert_eq!(col_b.value(1).to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn test_normalize_fixed_size_list_floats() {
        let values = Float64Array::from(vec![Some(-0.0), Some(f64::NAN), Some(1.0), Some(-0.0)]);
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let arr: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            2,
            Arc::new(values),
            None,
        ));

        let normalized = normalize_negative_zero(&arr);
        let normalized = normalized.as_fixed_size_list();
        let flat = normalized
            .values()
            .as_primitive::<arrow::datatypes::Float64Type>();

        assert_eq!(flat.value(0).to_bits(), 0.0f64.to_bits());
        assert!(flat.value(1).is_nan());
        assert_eq!(flat.value(2), 1.0);
        assert_eq!(flat.value(3).to_bits(), 0.0f64.to_bits());
    }
}
