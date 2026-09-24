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

//! Spark equality for nested floating-point values without materializing normalized columns.

use super::nested_float_normalize::{
    has_float_leaf, normalize_nested_floats, NormalizeNestedFloats,
};
use arrow::array::{make_comparator, Array, ArrayRef, AsArray, BooleanArray, OffsetSizeTrait};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{not, or_kleene, SortOptions};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{in_list, BinaryExpr, InListExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

type Equality = Box<dyn Fn(usize, usize) -> bool + Send + Sync>;

fn needs_spark_equality(dt: &DataType) -> bool {
    dt.is_nested() && has_float_leaf(dt)
}

// Inner nulls participate in structural equality. Outer SQL nulls are handled separately.
fn with_nulls(left: &ArrayRef, right: &ArrayRef, equal: Equality) -> Equality {
    if left.null_count() == 0 && right.null_count() == 0 {
        return equal;
    }
    let left = left.nulls().cloned();
    let right = right.nulls().cloned();
    Box::new(move |i, j| {
        let l = left.as_ref().is_some_and(|n| n.is_null(i));
        let r = right.as_ref().is_some_and(|n| n.is_null(j));
        if l || r {
            l == r
        } else {
            equal(i, j)
        }
    })
}

fn list_equality<O: OffsetSizeTrait>(left: &ArrayRef, right: &ArrayRef) -> Result<Equality> {
    let l = left.as_list::<O>();
    let r = right.as_list::<O>();
    let equal = nested_equality(l.values(), r.values())?;
    let l = l.offsets().clone();
    let r = r.offsets().clone();
    Ok(Box::new(move |i, j| {
        let (ls, le) = (l[i].as_usize(), l[i + 1].as_usize());
        let (rs, re) = (r[j].as_usize(), r[j + 1].as_usize());
        le - ls == re - rs && (ls..le).zip(rs..re).all(|(i, j)| equal(i, j))
    }))
}

fn nested_equality(left: &ArrayRef, right: &ArrayRef) -> Result<Equality> {
    if !DFSchema::datatype_is_logically_equal(left.data_type(), right.data_type()) {
        return internal_err!(
            "Nested equality requires matching types, got {} and {}",
            left.data_type(),
            right.data_type()
        );
    }
    if !has_float_leaf(left.data_type()) {
        let cmp = make_comparator(left.as_ref(), right.as_ref(), SortOptions::default())?;
        return Ok(Box::new(move |i, j| cmp(i, j).is_eq()));
    }
    let equal: Equality = match left.data_type() {
        DataType::Float32 => {
            let l = left.as_primitive::<Float32Type>().values().clone();
            let r = right.as_primitive::<Float32Type>().values().clone();
            Box::new(move |i, j| l[i] == r[j] || (l[i].is_nan() && r[j].is_nan()))
        }
        DataType::Float64 => {
            let l = left.as_primitive::<Float64Type>().values().clone();
            let r = right.as_primitive::<Float64Type>().values().clone();
            Box::new(move |i, j| l[i] == r[j] || (l[i].is_nan() && r[j].is_nan()))
        }
        DataType::List(_) => list_equality::<i32>(left, right)?,
        DataType::LargeList(_) => list_equality::<i64>(left, right)?,
        DataType::FixedSizeList(_, width) => {
            let l = left.as_fixed_size_list();
            let r = right.as_fixed_size_list();
            let equal = nested_equality(l.values(), r.values())?;
            let width = *width as usize;
            Box::new(move |i, j| (0..width).all(|k| equal(i * width + k, j * width + k)))
        }
        DataType::Struct(_) => {
            let equal = left
                .as_struct()
                .columns()
                .iter()
                .zip(right.as_struct().columns())
                .map(|(l, r)| nested_equality(l, r))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |i, j| equal.iter().all(|eq| eq(i, j)))
        }
        _ => return internal_err!("Unsupported nested equality type {}", left.data_type()),
    };
    Ok(with_nulls(left, right, equal))
}

struct Operand {
    array: ArrayRef,
    scalar: bool,
}

impl Operand {
    fn new(value: ColumnarValue) -> Result<Self> {
        Ok(match value {
            ColumnarValue::Array(array) => Self {
                array,
                scalar: false,
            },
            ColumnarValue::Scalar(value) => Self {
                array: value.to_array()?,
                scalar: true,
            },
        })
    }

    fn equal(&self, other: &Self, rows: usize) -> Result<ColumnarValue> {
        let scalar = self.scalar && other.scalar;
        let len = if scalar { 1 } else { rows };
        // Scalars use a single value; only the Boolean result is broadcast across rows.
        let scalar_null =
            (self.scalar && self.array.is_null(0)) || (other.scalar && other.array.is_null(0));
        let result = if scalar_null
            || self.array.data_type() == &DataType::Null
            || other.array.data_type() == &DataType::Null
        {
            BooleanArray::new_null(len)
        } else {
            let equal = nested_equality(&self.array, &other.array)?;
            let nulls = match (self.scalar, other.scalar) {
                (true, true) => None,
                (true, false) => other.array.nulls().cloned(),
                (false, true) => self.array.nulls().cloned(),
                (false, false) => NullBuffer::union(self.array.nulls(), other.array.nulls()),
            };
            let values = BooleanBuffer::collect_bool(len, |row| {
                equal(
                    if self.scalar { 0 } else { row },
                    if other.scalar { 0 } else { row },
                )
            });
            BooleanArray::new(values, nulls)
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                if result.is_null(0) {
                    None
                } else {
                    Some(result.value(0))
                },
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Equality and dynamic membership share the same SQL null and nested equality rules.
#[derive(Debug, Eq)]
struct NestedPredicate {
    value: Arc<dyn PhysicalExpr>,
    candidates: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
    membership: bool,
}

impl PartialEq for NestedPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
            && self.candidates == other.candidates
            && self.negated == other.negated
            && self.membership == other.membership
    }
}

impl Hash for NestedPredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.candidates.hash(state);
        self.negated.hash(state);
        self.membership.hash(state);
    }
}

impl Display for NestedPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Nested{}({}, {:?}, negated={})",
            if self.membership { "In" } else { "Equal" },
            self.value,
            self.candidates,
            self.negated
        )
    }
}

impl PhysicalExpr for NestedPredicate {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        for child in self.children() {
            if child.nullable(schema)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = Operand::new(self.value.evaluate(batch)?)?;
        let mut found = None;
        for candidate in &self.candidates {
            let next = value.equal(&Operand::new(candidate.evaluate(batch)?)?, batch.num_rows())?;
            found = Some(match found {
                None => next,
                Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))) => next,
                Some(previous) => {
                    let scalar = matches!(&previous, ColumnarValue::Scalar(_))
                        && matches!(&next, ColumnarValue::Scalar(_));
                    let len = if scalar { 1 } else { batch.num_rows() };
                    let l = previous.into_array(len)?;
                    let r = next.into_array(len)?;
                    let result = or_kleene(l.as_boolean(), r.as_boolean())?;
                    if scalar {
                        ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
                    } else {
                        ColumnarValue::Array(Arc::new(result))
                    }
                }
            });
            let done = match found.as_ref().unwrap() {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => true,
                ColumnarValue::Array(a) => a.null_count() == 0 && !a.as_boolean().has_false(),
                _ => false,
            };
            if done {
                break;
            }
        }
        let found = found.unwrap_or(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))));
        if !self.negated {
            return Ok(found);
        }
        match found {
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(v.map(|v| !v))))
            }
            ColumnarValue::Array(a) => Ok(ColumnarValue::Array(Arc::new(not(a.as_boolean())?))),
            _ => internal_err!("Nested predicate must return Boolean"),
        }
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        std::iter::once(&self.value)
            .chain(self.candidates.iter())
            .collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.candidates.len() + 1 {
            return internal_err!("Invalid nested predicate child count");
        }
        let value = children.remove(0);
        Ok(Arc::new(Self {
            value,
            candidates: children,
            negated: self.negated,
            membership: self.membership,
        }))
    }
}

/// Build equality after the planner has reconciled nested operand nullability.
pub fn spark_comparison(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // An operand whose type does not resolve against this schema falls back to the plain
    // comparison, the way `reconcile_nested_comparison_types` already leaves such operands alone.
    let nested = matches!(op, Operator::Eq | Operator::NotEq)
        && match (left.data_type(schema), right.data_type(schema)) {
            (Ok(lt), Ok(_)) => needs_spark_equality(&lt),
            _ => false,
        };
    if nested {
        validate_types(&left, std::slice::from_ref(&right), schema)?;
        Ok(Arc::new(NestedPredicate {
            value: left,
            candidates: vec![right],
            negated: op == Operator::NotEq,
            membership: false,
        }))
    } else {
        Ok(Arc::new(BinaryExpr::new(left, op, right)))
    }
}

fn validate_types(
    value: &Arc<dyn PhysicalExpr>,
    candidates: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<()> {
    let dt = value.data_type(schema)?;
    for candidate in candidates {
        let other = candidate.data_type(schema)?;
        if other != DataType::Null && !DFSchema::datatype_is_logically_equal(&dt, &other) {
            return internal_err!("Nested predicate requires matching types, got {dt} and {other}");
        }
    }
    Ok(())
}

/// Keep canonicalized static filters, but never normalize whole dynamic nested columns.
pub fn spark_in_list(
    value: Arc<dyn PhysicalExpr>,
    candidates: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if !needs_spark_equality(&value.data_type(schema)?) || candidates.is_empty() {
        return in_list(value, candidates, &negated, schema);
    }
    validate_types(&value, &candidates, schema)?;
    let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
    let constants = candidates
        .iter()
        .map(|child| {
            if is_volatile(child) {
                return None;
            }
            match child.evaluate(&empty).ok()? {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            }
        })
        .collect::<Option<Vec<_>>>();
    if let Some(constants) = constants {
        // Untyped NULL candidates are legal alongside typed nested constants.
        let data_type = value.data_type(schema)?;
        let constants = constants
            .into_iter()
            .map(|v| {
                if v == ScalarValue::Null {
                    ScalarValue::try_from(&data_type)
                } else {
                    Ok(v)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let values = ScalarValue::iter_to_array(constants)?;
        return Ok(Arc::new(InListExpr::try_new_from_array(
            NormalizeNestedFloats::wrap_if_needed(value, schema)?,
            normalize_nested_floats(&values),
            negated,
            schema,
        )?));
    }
    Ok(Arc::new(NestedPredicate {
        value,
        candidates,
        negated,
        membership: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        FixedSizeListArray, Float64Array, Int32Array, LargeListArray, ListArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use std::collections::hash_map::DefaultHasher;

    fn literal(array: &ArrayRef, row: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(
            ScalarValue::try_from_array(array, row).unwrap(),
        ))
    }
    fn results(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<Option<bool>> {
        expr.evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
            .as_boolean()
            .iter()
            .collect()
    }
    fn batch(left: ArrayRef, right: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", left.data_type().clone(), true),
            Field::new("b", right.data_type().clone(), true),
        ]));
        RecordBatch::try_new(schema, vec![left, right]).unwrap()
    }
    fn columns() -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
        (Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1)))
    }

    #[test]
    fn nested_shapes_and_slices() -> Result<()> {
        let l: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0xfff8000000000001)),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(1.0),
        ]));
        let r: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(f64::from_bits(0x7ff8000000000002)),
            None,
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            Some(2.0),
        ]));
        for shape in 0..6 {
            let nest = |a: ArrayRef| -> ArrayRef {
                let field = Arc::new(Field::new("item", a.data_type().clone(), true));
                match shape {
                    0 => Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::from_lengths([1; 6]),
                        a,
                        None,
                    )),
                    1 => Arc::new(LargeListArray::new(
                        field,
                        OffsetBuffer::from_lengths([1; 6]),
                        a,
                        None,
                    )),
                    2 => Arc::new(FixedSizeListArray::new(field, 1, a, None)),
                    3 => Arc::new(StructArray::new(
                        vec![field, Arc::new(Field::new("int", DataType::Int32, false))].into(),
                        vec![a, Arc::new(Int32Array::from(vec![7; 6]))],
                        None,
                    )),
                    _ => {
                        let list: ArrayRef = Arc::new(ListArray::new(
                            field,
                            OffsetBuffer::from_lengths([1; 6]),
                            a,
                            None,
                        ));
                        let field = Arc::new(Field::new("item", list.data_type().clone(), true));
                        if shape == 4 {
                            Arc::new(ListArray::new(
                                field,
                                OffsetBuffer::from_lengths([1; 6]),
                                list,
                                None,
                            ))
                        } else {
                            Arc::new(StructArray::new(vec![field].into(), vec![list], None))
                        }
                    }
                }
            };
            for sliced in [false, true] {
                let (mut left, mut right) = (nest(Arc::clone(&l)), nest(Arc::clone(&r)));
                let expected = if sliced {
                    left = left.slice(1, 5);
                    right = right.slice(1, 5);
                    vec![Some(true), Some(true), Some(true), Some(false), Some(false)]
                } else {
                    vec![
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(false),
                        Some(false),
                    ]
                };
                let batch = batch(left, right);
                let (a, b) = columns();
                for negated in [false, true] {
                    let expected: Vec<_> =
                        expected.iter().map(|v| v.map(|v| v ^ negated)).collect();
                    let eq = spark_comparison(
                        Arc::clone(&a),
                        if negated {
                            Operator::NotEq
                        } else {
                            Operator::Eq
                        },
                        Arc::clone(&b),
                        batch.schema().as_ref(),
                    )?;
                    let inside = spark_in_list(
                        Arc::clone(&a),
                        vec![Arc::clone(&b)],
                        negated,
                        batch.schema().as_ref(),
                    )?;
                    assert_eq!(results(&eq, &batch), expected);
                    assert_eq!(results(&inside, &batch), expected);
                }
                for row in 0..batch.num_rows() {
                    let lit = literal(batch.column(1), row);
                    let eq = spark_comparison(
                        Arc::clone(&a),
                        Operator::Eq,
                        Arc::clone(&lit),
                        batch.schema().as_ref(),
                    )?;
                    let inside = spark_in_list(
                        Arc::clone(&a),
                        vec![Arc::clone(&lit)],
                        false,
                        batch.schema().as_ref(),
                    )?;
                    assert_eq!(results(&eq, &batch), results(&inside, &batch));
                    let reversed = spark_comparison(
                        lit,
                        Operator::Eq,
                        Arc::clone(&a),
                        batch.schema().as_ref(),
                    )?;
                    assert_eq!(results(&eq, &batch), results(&reversed, &batch));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn nested_membership_nulls_and_mixed_candidates() -> Result<()> {
        let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(-0.0)]),
            Some(vec![Some(4.0)]),
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(f32::from_bits(0xffc00001))]),
        ]));
        let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(0.0)]),
            None,
            Some(vec![None]),
            Some(vec![]),
            None,
            Some(vec![Some(1.0)]),
            Some(vec![Some(f32::from_bits(0x7fc00002))]),
        ]));
        let batch = batch(left, right);
        let (a, b) = columns();
        let zero = literal(batch.column(1), 0);
        let null: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::try_from(
            batch.column(0).data_type(),
        )?));
        for negated in [false, true] {
            for candidates in [
                vec![Arc::clone(&b)],
                vec![Arc::clone(&zero), Arc::clone(&b)],
            ] {
                let expr =
                    spark_in_list(Arc::clone(&a), candidates, negated, batch.schema().as_ref())?;
                let expected = [
                    Some(true),
                    None,
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ]
                .map(|v| v.map(|v| v ^ negated));
                assert_eq!(results(&expr, &batch), expected);
            }
            let expr = spark_in_list(
                Arc::clone(&a),
                vec![Arc::clone(&null), Arc::clone(&b)],
                negated,
                batch.schema().as_ref(),
            )?;
            assert_eq!(
                results(&expr, &batch),
                [
                    Some(!negated),
                    None,
                    Some(!negated),
                    Some(!negated),
                    None,
                    None,
                    Some(!negated)
                ]
            );
            let expr = spark_in_list(
                Arc::clone(&a),
                vec![Arc::clone(&zero), Arc::clone(&null)],
                negated,
                batch.schema().as_ref(),
            )?;
            assert_eq!(
                results(&expr, &batch),
                [Some(!negated), None, None, None, None, None, None]
            );
        }
        let scalar = spark_comparison(
            Arc::clone(&zero),
            Operator::Eq,
            Arc::clone(&zero),
            batch.schema().as_ref(),
        )?;
        assert!(matches!(
            scalar.evaluate(&RecordBatch::new_empty(batch.schema()))?,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        let eq = spark_comparison(
            Arc::clone(&a),
            Operator::Eq,
            Arc::clone(&b),
            batch.schema().as_ref(),
        )?;
        assert!(eq.nullable(batch.schema().as_ref())?);
        assert_eq!(eq.data_type(batch.schema().as_ref())?, DataType::Boolean);
        let rebuilt = Arc::clone(&eq).with_new_children(vec![a, b])?;
        assert!(eq.eq(&rebuilt));
        let hash = |e: &Arc<dyn PhysicalExpr>| {
            let mut h = DefaultHasher::new();
            e.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&eq), hash(&rebuilt));
        assert!(eq.with_new_children(vec![zero]).is_err());
        Ok(())
    }
    #[test]
    fn nullability_and_untyped_null_candidates() -> Result<()> {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 1.0]));
        let l: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, false)),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::clone(&values),
            None,
        ));
        let r: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::from_lengths([1, 1]),
            values,
            None,
        ));
        let batch = batch(l, r);
        let (a, b) = columns();
        let null: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Null));
        let dynamic = spark_in_list(
            Arc::clone(&a),
            vec![Arc::clone(&null), b],
            false,
            batch.schema().as_ref(),
        )?;
        assert_eq!(results(&dynamic, &batch), vec![Some(true), Some(true)]);
        let constant = spark_in_list(
            Arc::clone(&a),
            vec![null, literal(batch.column(0), 0)],
            false,
            batch.schema().as_ref(),
        )?;
        assert_eq!(results(&constant, &batch), vec![Some(true), None]);
        assert_eq!(
            results(&dynamic, &RecordBatch::new_empty(batch.schema())),
            vec![]
        );
        let primitive: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        assert!(spark_comparison(
            Arc::clone(&primitive),
            Operator::Eq,
            primitive,
            batch.schema().as_ref()
        )?
        .as_ref()
        .is::<BinaryExpr>());
        // An operand that does not resolve against this schema must not fail planning:
        // it falls back to BinaryExpr, the behaviour before the nested path existed.
        let unresolved: Arc<dyn PhysicalExpr> = Arc::new(Column::new("missing", 99));
        assert!(unresolved.data_type(batch.schema().as_ref()).is_err());
        for (l, r) in [
            (Arc::clone(&unresolved), Arc::clone(&a)),
            (Arc::clone(&a), Arc::clone(&unresolved)),
        ] {
            assert!(
                spark_comparison(l, Operator::Eq, r, batch.schema().as_ref())?
                    .as_ref()
                    .is::<BinaryExpr>()
            );
        }
        Ok(())
    }

    // A stateful test expression makes planning-time and runtime evaluation observable.
    #[derive(Debug)]
    struct Probe {
        child: Arc<dyn PhysicalExpr>,
        volatile: bool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Eq for Probe {}
    impl PartialEq for Probe {
        fn eq(&self, other: &Self) -> bool {
            self.child.eq(&other.child) && self.volatile == other.volatile
        }
    }
    impl Hash for Probe {
        fn hash<H: Hasher>(&self, h: &mut H) {
            self.child.hash(h);
            self.volatile.hash(h);
        }
    }
    impl Display for Probe {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Probe({})", self.child)
        }
    }
    impl PhysicalExpr for Probe {
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
        fn data_type(&self, s: &Schema) -> Result<DataType> {
            self.child.data_type(s)
        }
        fn nullable(&self, s: &Schema) -> Result<bool> {
            self.child.nullable(s)
        }
        fn evaluate(&self, b: &RecordBatch) -> Result<ColumnarValue> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.child.evaluate(b)
        }
        fn is_volatile_node(&self) -> bool {
            self.volatile
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.child]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(Self {
                child: Arc::clone(&children[0]),
                volatile: self.volatile,
                calls: Arc::clone(&self.calls),
            }))
        }
    }

    #[test]
    fn constants_evaluated_once_and_volatile_candidates_remain_dynamic() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let array: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                vec![Some(-0.0)],
            )]));
        let batch = batch(Arc::clone(&array), array);
        for volatile in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let probe: Arc<dyn PhysicalExpr> = Arc::new(Probe {
                child: literal(batch.column(0), 0),
                volatile,
                calls: Arc::clone(&calls),
            });
            // Volatility must also be recognized through a nonvolatile parent node.
            let probe = NormalizeNestedFloats::wrap_if_needed(probe, batch.schema().as_ref())?;
            let (a, _) = columns();
            let expr = spark_in_list(a, vec![probe], false, batch.schema().as_ref())?;
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!volatile));
            assert_eq!(results(&expr, &batch), vec![Some(true)]);
            assert_eq!(results(&expr, &batch), vec![Some(true)]);
            assert_eq!(calls.load(Ordering::SeqCst), if volatile { 2 } else { 1 });
        }
        Ok(())
    }
    #[test]
    fn dynamic_input_is_evaluated_once_and_runtime_errors_propagate() -> Result<()> {
        use datafusion::physical_expr::expressions::CastExpr;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let l: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            vec![Some(1.0)],
        )]));
        let r: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
            vec![Some(2.0)],
        )]));
        let batch = batch(l, r);
        let (a, b) = columns();
        let calls = Arc::new(AtomicUsize::new(0));
        let value: Arc<dyn PhysicalExpr> = Arc::new(Probe {
            child: Arc::clone(&a),
            volatile: false,
            calls: Arc::clone(&calls),
        });
        let bad: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Literal::new(ScalarValue::Utf8(Some("bad".into())))),
            batch.column(0).data_type().clone(),
            None,
        ));
        let expr = spark_in_list(
            value,
            vec![b, literal(batch.column(0), 0), Arc::clone(&bad)],
            false,
            batch.schema().as_ref(),
        )?;
        assert_eq!(results(&expr, &batch), vec![Some(true)]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Failure during the empty-batch probe selects the dynamic path, not a false result.
        let expr = spark_in_list(a, vec![bad], false, batch.schema().as_ref())?;
        assert!(expr.evaluate(&batch).is_err());
        Ok(())
    }
}
