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

use crate::math_funcs::internal::normalize_float;
use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Float32Array, Float64Array, LargeListArray,
    ListArray, StructArray,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use std::sync::Arc;

pub(super) fn has_float_leaf(dt: &DataType) -> bool {
    match dt {
        DataType::Float32 | DataType::Float64 => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            has_float_leaf(field.data_type())
        }
        DataType::Struct(fields) => fields.iter().any(|f| has_float_leaf(f.data_type())),
        _ => false,
    }
}

/// Recursively rebuilds nested arrays with `-0.0` normalized to `0.0` and NaN canonicalized
/// in any Float32/Float64 leaves.
///
/// Subtrees without a float leaf are returned as is. The guard is applied at every level, not
/// just by the caller, so that a float-free sibling of a float field is never rebuilt. That
/// keeps the rebuild proportional to the float data, and it also leaves empty structs alone:
/// `StructArray::new` cannot infer a length from zero columns and would panic. An empty struct
/// reaches this code through Iceberg's `_partition` metadata column on an unpartitioned table.
pub(super) fn normalize_nested_floats(array: &ArrayRef) -> ArrayRef {
    if !has_float_leaf(array.data_type()) {
        return Arc::clone(array);
    }

    match array.data_type() {
        DataType::Float32 => {
            let normalized: Float32Array =
                array.as_primitive::<Float32Type>().unary(normalize_float);
            Arc::new(normalized)
        }
        DataType::Float64 => {
            let normalized: Float64Array =
                array.as_primitive::<Float64Type>().unary(normalize_float);
            Arc::new(normalized)
        }
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let normalized_values = normalize_nested_floats(list.values());
            Arc::new(ListArray::new(
                Arc::clone(field),
                list.offsets().clone(),
                normalized_values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            let normalized_values = normalize_nested_floats(list.values());
            Arc::new(LargeListArray::new(
                Arc::clone(field),
                list.offsets().clone(),
                normalized_values,
                list.nulls().cloned(),
            ))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let normalized_values = normalize_nested_floats(list.values());
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
                s.columns().iter().map(normalize_nested_floats).collect();
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
    use arrow::array::Int32Array;
    use arrow::array::ListBuilder;
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn test_has_float_leaf() {
        assert!(has_float_leaf(&DataType::Float64));
        assert!(has_float_leaf(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            true
        )))));
        assert!(has_float_leaf(&DataType::Struct(
            vec![
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Field::new("b", DataType::Float64, true)),
            ]
            .into()
        )));
        assert!(!has_float_leaf(&DataType::Int32));
        assert!(!has_float_leaf(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true
        )))));
    }

    #[test]
    fn test_normalize_flat_floats() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(-f64::NAN),
            None,
            Some(1.5),
        ]));
        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_primitive::<Float64Type>();

        assert_eq!(normalized.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(normalized.value(1).to_bits(), 0.0f64.to_bits());
        assert_eq!(normalized.value(2).to_bits(), f64::NAN.to_bits());
        assert_eq!(normalized.value(3).to_bits(), f64::NAN.to_bits());
        assert!(normalized.is_null(4));
        assert_eq!(normalized.value(5), 1.5);
    }

    #[test]
    fn test_normalize_nested_list_floats() {
        let mut builder = ListBuilder::new(Float64Builder::new());
        builder.values().append_value(-0.0);
        builder.values().append_value(-f64::NAN);
        builder.append(true);
        let arr: ArrayRef = Arc::new(builder.finish());

        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_list::<i32>();
        let inner = normalized.value(0);
        let inner = inner.as_primitive::<Float64Type>();

        assert_eq!(inner.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(inner.value(1).to_bits(), f64::NAN.to_bits());
    }

    /// An empty struct sibling of a float field must survive normalization. Iceberg exposes
    /// `_partition` as `struct<>` on an unpartitioned table, and rebuilding it would panic
    /// because `StructArray::new` cannot infer a length from zero columns.
    #[test]
    fn test_normalize_struct_with_empty_struct_sibling() {
        let x = Float64Array::from(vec![Some(-0.0), Some(1.0)]);
        let partition = StructArray::new_empty_fields(2, None);
        let fields = vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("p", partition.data_type().clone(), true)),
        ];
        let arr: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![Arc::new(x), Arc::new(partition)],
            None,
        ));

        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_struct();

        let col_x = normalized.column(0).as_primitive::<Float64Type>();
        assert_eq!(col_x.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(col_x.value(1), 1.0);

        let col_p = normalized.column(1).as_struct();
        assert_eq!(col_p.num_columns(), 0);
        assert_eq!(col_p.len(), 2);
    }

    /// A float-free subtree is returned as is rather than rebuilt. The child here is a nested
    /// struct, which the recursive arms would otherwise rebuild into a fresh array.
    #[test]
    fn test_normalize_leaves_float_free_subtree_untouched() {
        let inner_fields: Fields = vec![Arc::new(Field::new("i", DataType::Int32, true))].into();
        let inner: ArrayRef = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2)]))],
            None,
        ));
        let floats = Float64Array::from(vec![Some(-0.0), Some(1.0)]);
        let fields: Fields = vec![
            Arc::new(Field::new("s", DataType::Struct(inner_fields), true)),
            Arc::new(Field::new("f", DataType::Float64, true)),
        ]
        .into();
        let arr: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::clone(&inner), Arc::new(floats)],
            None,
        ));

        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_struct();

        assert!(Arc::ptr_eq(normalized.column(0), &inner));
        assert_eq!(
            normalized
                .column(1)
                .as_primitive::<Float64Type>()
                .value(0)
                .to_bits(),
            0.0f64.to_bits()
        );
    }

    #[test]
    fn test_normalize_struct_floats() {
        let a = Float64Array::from(vec![Some(-0.0), Some(1.0)]);
        let b = Float64Array::from(vec![Some(-f64::NAN), Some(-0.0)]);
        let fields = vec![
            Arc::new(Field::new("a", DataType::Float64, true)),
            Arc::new(Field::new("b", DataType::Float64, true)),
        ];
        let arr: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![Arc::new(a), Arc::new(b)],
            None,
        ));

        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_struct();
        let col_a = normalized.column(0).as_primitive::<Float64Type>();
        let col_b = normalized.column(1).as_primitive::<Float64Type>();

        assert_eq!(col_a.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(col_a.value(1), 1.0);
        assert_eq!(col_b.value(0).to_bits(), f64::NAN.to_bits());
        assert_eq!(col_b.value(1).to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn test_normalize_fixed_size_list_floats() {
        let values = Float64Array::from(vec![Some(-0.0), Some(-f64::NAN), Some(1.0), Some(-0.0)]);
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let arr: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::clone(&field),
            2,
            Arc::new(values),
            None,
        ));

        let normalized = normalize_nested_floats(&arr);
        let normalized = normalized.as_fixed_size_list();
        let flat = normalized.values().as_primitive::<Float64Type>();

        assert_eq!(flat.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(flat.value(1).to_bits(), f64::NAN.to_bits());
        assert_eq!(flat.value(2), 1.0);
        assert_eq!(flat.value(3).to_bits(), 0.0f64.to_bits());
    }
}
