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
use arrow::datatypes::{DataType, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Normalizes nested IN operands, preserving constants for static membership lookup.
#[derive(Debug, Eq)]
pub struct NormalizeNestedFloats {
    child: Arc<dyn PhysicalExpr>,
}

impl NormalizeNestedFloats {
    /// Wrap nested floating-point operands only; scalar floats keep their existing semantics.
    pub fn wrap_if_needed(
        child: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let dt = child.data_type(schema)?;
        if matches!(
            dt,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
        ) && has_float_leaf(&dt)
        {
            Ok(Arc::new(Self { child }))
        } else {
            Ok(child)
        }
    }
}

impl PartialEq for NormalizeNestedFloats {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Hash for NormalizeNestedFloats {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl Display for NormalizeNestedFloats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NormalizeNestedFloats({})", self.child)
    }
}

impl PhysicalExpr for NormalizeNestedFloats {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.child.data_type(schema)
    }

    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.child.nullable(schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.child.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(normalize_nested_floats(&array)))
            }
            ColumnarValue::Scalar(value) => {
                let array = normalize_nested_floats(&value.to_array()?);
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!("NormalizeNestedFloats expects one child");
        }
        Ok(Arc::new(Self {
            child: Arc::clone(&children[0]),
        }))
    }
}

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
    fn test_nested_membership_shapes() -> Result<()> {
        use arrow::array::BooleanArray;
        use datafusion::physical_expr::expressions::{in_list, Column, Literal};
        let left32: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(-0.0)]),
            Some(vec![Some(0.0)]),
            Some(vec![Some(f32::from_bits(0xffc00001))]),
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(3.0)]),
        ]));
        let right32: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(0.0)]),
            Some(vec![Some(-0.0)]),
            Some(vec![Some(f32::from_bits(0x7fc00002))]),
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(4.0)]),
        ]));
        let left64: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(-0.0)]),
            Some(vec![Some(0.0)]),
            Some(vec![Some(f64::from_bits(0xfff8000000000001))]),
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(3.0)]),
        ]));
        let right64: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(0.0)]),
            Some(vec![Some(-0.0)]),
            Some(vec![Some(f64::from_bits(0x7ff8000000000002))]),
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(4.0)]),
        ]));
        for (left, right) in [(left32, right32), (left64, right64)] {
            // Test lists directly, then lists nested inside another list and a struct.
            for depth in 0..3 {
                let nest = |array: ArrayRef| -> ArrayRef {
                    match depth {
                        0 => array,
                        1 => Arc::new(ListArray::new(
                            Arc::new(Field::new("item", array.data_type().clone(), true)),
                            arrow::buffer::OffsetBuffer::from_lengths(std::iter::repeat_n(
                                1,
                                array.len(),
                            )),
                            Arc::clone(&array),
                            array.nulls().cloned(),
                        )),
                        _ => Arc::new(StructArray::new(
                            vec![Arc::new(Field::new("v", array.data_type().clone(), true))].into(),
                            vec![Arc::clone(&array)],
                            array.nulls().cloned(),
                        )),
                    }
                };
                let left = nest(Arc::clone(&left));
                let right = nest(Arc::clone(&right));
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", left.data_type().clone(), true),
                    Field::new("b", right.data_type().clone(), true),
                ]));
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), vec![left, Arc::clone(&right)])?;
                let a =
                    NormalizeNestedFloats::wrap_if_needed(Arc::new(Column::new("a", 0)), &schema)?;
                let b =
                    NormalizeNestedFloats::wrap_if_needed(Arc::new(Column::new("b", 1)), &schema)?;
                for negated in [false, true] {
                    let expr = in_list(Arc::clone(&a), vec![Arc::clone(&b)], &negated, &schema)?;
                    let actual = expr.evaluate(&batch)?.into_array(7)?;
                    let expected = BooleanArray::from(vec![
                        Some(!negated),
                        Some(!negated),
                        Some(!negated),
                        Some(!negated),
                        Some(!negated),
                        None,
                        Some(negated),
                    ]);
                    assert_eq!(actual.as_boolean(), &expected);
                    // Constant candidates exercise the static hash lookup, including distinct NaN bits.
                    for row in [0, 1, 2, 3, 4, 6] {
                        let literal =
                            Arc::new(Literal::new(ScalarValue::try_from_array(&right, row)?));
                        let candidate = NormalizeNestedFloats::wrap_if_needed(literal, &schema)?;
                        let expr = in_list(Arc::clone(&a), vec![candidate], &negated, &schema)?;
                        let actual = expr.evaluate(&batch)?.into_array(7)?;
                        assert_eq!(actual.as_boolean().value(row), expected.value(row));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_membership_wrapper() -> Result<()> {
        use datafusion::physical_expr::expressions::{in_list, Column, Literal};
        // Noncanonical NaNs and negative zero must normalize identically on both paths.
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0x7ff8000000000001)),
            None,
            Some(2.0),
        ]));
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("v", DataType::Float64, true))].into(),
            vec![values],
            None,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            input.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&input)])?;
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let wrapped = NormalizeNestedFloats::wrap_if_needed(Arc::clone(&column), &schema)?;
        assert_eq!(wrapped.data_type(&schema)?, column.data_type(&schema)?);
        assert_eq!(wrapped.nullable(&schema)?, column.nullable(&schema)?);
        assert!(matches!(wrapped.evaluate(&batch)?, ColumnarValue::Array(_)));
        for row in 0..input.len() {
            let literal: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::try_from_array(&input, row)?));
            let literal = NormalizeNestedFloats::wrap_if_needed(literal, &schema)?;
            assert!(matches!(
                literal.evaluate(&RecordBatch::new_empty(Arc::clone(&schema)))?,
                ColumnarValue::Scalar(_)
            ));
            let expr = in_list(Arc::clone(&wrapped), vec![literal], &false, &schema)?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert!(result.as_boolean().value(row));
        }
        let plain: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        assert!(Arc::ptr_eq(
            &plain,
            &NormalizeNestedFloats::wrap_if_needed(Arc::clone(&plain), &schema)?
        ));
        Ok(())
    }

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
