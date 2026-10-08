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

//! Spark comparisons for floating-point values, and Spark equality for nested floating-point
//! values without materializing normalized columns.

use crate::float_semantics::{
    is_nested_with_float_leaf, normalize_comparison_operand, normalize_nested_floats,
    spark_equality, NormalizeNestedFloats,
};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{not, or_kleene};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{in_list, BinaryExpr, Column, InListExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
            // Inner nulls take part in structural equality. Outer SQL nulls are handled here.
            let equal = spark_equality(self.array.as_ref(), other.array.as_ref())?;
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

/// How [`spark_comparison`] treats floating-point operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatOperands {
    /// Normalize them, so that the comparison follows Spark's SQL ordering.
    Normalize,
    /// Leave a Float32 or Float64 column compared with a literal other than NaN as it is, and
    /// normalize every other operand. Only a scan's pushed-down data filters use this, and only
    /// when the Parquet reader prunes with them but does not filter rows: Parquet pruning
    /// recognizes a column compared with a literal but not a normalized column, and Spark's
    /// Filter above the scan applies Spark's semantics to every row. A NaN literal is normalized
    /// with the column, giving up pruning, because a stored NaN can have other bits than the
    /// literal and a writer can leave NaNs out of the column statistics. With row-level pushdown
    /// the reader would drop the rows such a comparison rejects, including a stored NaN with the
    /// sign bit set, which Arrow orders below every other value, so the data filters use
    /// [`FloatOperands::Normalize`] there instead.
    Raw,
}

/// Builds a comparison with Spark's SQL ordering for floats, in which `-0.0` equals `0.0`, all
/// NaNs are equal and NaN sorts above every other value, at any depth of a list or struct.
///
/// Arrow compares floats by IEEE 754 total order instead, so each float operand is normalized
/// first with [`normalize_comparison_operand`], after which the two orders agree. Nested `=` and
/// `<>` compare with `spark_equality` instead, without building normalized copies of the nested
/// values. Any other operator, such as `AND`, builds a plain [`BinaryExpr`].
///
/// The planner reconciles the nullability of nested operands before calling this.
pub fn spark_comparison(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    float_operands: FloatOperands,
) -> Result<Arc<dyn PhysicalExpr>> {
    use Operator::*;
    if !matches!(
        op,
        Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom
    ) {
        return Ok(Arc::new(BinaryExpr::new(left, op, right)));
    }
    // An operand whose type does not resolve against this schema falls back to the plain
    // comparison, the way `reconcile_nested_comparison_types` already leaves such operands alone.
    let (Ok(left_type), Ok(_)) = (left.data_type(schema), right.data_type(schema)) else {
        return Ok(Arc::new(BinaryExpr::new(left, op, right)));
    };
    if matches!(op, Eq | NotEq) && is_nested_with_float_leaf(&left_type) {
        validate_types(&left, std::slice::from_ref(&right), schema)?;
        return Ok(Arc::new(NestedPredicate {
            value: left,
            candidates: vec![right],
            negated: op == NotEq,
            membership: false,
        }));
    }
    if float_operands == FloatOperands::Raw {
        if let Some(comparison) = raw_float_comparison(&left, op, &right, schema) {
            return Ok(comparison);
        }
    }
    Ok(Arc::new(BinaryExpr::new(
        normalize_comparison_operand(left, schema)?,
        op,
        normalize_comparison_operand(right, schema)?,
    )))
}

/// The comparison that [`FloatOperands::Raw`] builds for a Float32 or Float64 column compared with
/// a literal other than NaN: the column and the literal as they are. `None` for any other operands.
///
/// Statistics pruning compares `-0.0` and `0.0` as equal, but a bloom filter probe hashes the
/// literal's bits, so `=` against a zero literal becomes `=` against either zero.
fn raw_float_comparison(
    left: &Arc<dyn PhysicalExpr>,
    op: Operator,
    right: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Option<Arc<dyn PhysicalExpr>> {
    let (literal, literal_on_left) = match (
        left.downcast_ref::<Literal>(),
        right.downcast_ref::<Literal>(),
    ) {
        (None, Some(literal)) if is_float_column(left, schema) => (literal, false),
        (Some(literal), None) if is_float_column(right, schema) => (literal, true),
        _ => return None,
    };
    let (negative_zero, positive_zero) = match literal.value() {
        ScalarValue::Float32(Some(v)) if v.is_nan() => return None,
        ScalarValue::Float64(Some(v)) if v.is_nan() => return None,
        ScalarValue::Float32(Some(v)) if op == Operator::Eq && *v == 0.0 => (
            ScalarValue::Float32(Some(-0.0)),
            ScalarValue::Float32(Some(0.0)),
        ),
        ScalarValue::Float64(Some(v)) if op == Operator::Eq && *v == 0.0 => (
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Float64(Some(0.0)),
        ),
        _ => {
            return Some(Arc::new(BinaryExpr::new(
                Arc::clone(left),
                op,
                Arc::clone(right),
            )))
        }
    };
    let equal_to = |zero: ScalarValue| -> Arc<dyn PhysicalExpr> {
        let zero: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(zero));
        if literal_on_left {
            Arc::new(BinaryExpr::new(zero, op, Arc::clone(right)))
        } else {
            Arc::new(BinaryExpr::new(Arc::clone(left), op, zero))
        }
    };
    Some(Arc::new(BinaryExpr::new(
        equal_to(negative_zero),
        Operator::Or,
        equal_to(positive_zero),
    )))
}

/// Whether `expr` is a Float32 or Float64 column, the operand that Parquet pruning reads.
fn is_float_column(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    expr.downcast_ref::<Column>().is_some()
        && matches!(
            expr.data_type(schema),
            Ok(DataType::Float32 | DataType::Float64)
        )
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
    if !is_nested_with_float_leaf(&value.data_type(schema)?) || candidates.is_empty() {
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
    use arrow::datatypes::{Field, Float32Type, Float64Type};
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
                        FloatOperands::Normalize,
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
                        FloatOperands::Normalize,
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
                        FloatOperands::Normalize,
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
            FloatOperands::Normalize,
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
            FloatOperands::Normalize,
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
            batch.schema().as_ref(),
            FloatOperands::Normalize,
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
            assert!(spark_comparison(
                l,
                Operator::Eq,
                r,
                batch.schema().as_ref(),
                FloatOperands::Normalize
            )?
            .as_ref()
            .is::<BinaryExpr>());
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

    const COMPARISONS: [Operator; 8] = [
        Operator::Eq,
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
        Operator::IsDistinctFrom,
        Operator::IsNotDistinctFrom,
    ];

    /// Spark's answer for `left op right`, given the ordering of two non-null values.
    fn spark_answer(op: Operator, ordering: Option<Option<std::cmp::Ordering>>) -> Option<bool> {
        use Operator::*;
        match ordering {
            // Both non-null.
            Some(Some(ord)) => Some(match op {
                Eq | IsNotDistinctFrom => ord.is_eq(),
                NotEq | IsDistinctFrom => ord.is_ne(),
                Lt => ord.is_lt(),
                LtEq => ord.is_le(),
                Gt => ord.is_gt(),
                GtEq => ord.is_ge(),
                _ => unreachable!(),
            }),
            // Both null.
            Some(None) => match op {
                IsNotDistinctFrom => Some(true),
                IsDistinctFrom => Some(false),
                _ => None,
            },
            // One null.
            None => match op {
                IsNotDistinctFrom => Some(false),
                IsDistinctFrom => Some(true),
                _ => None,
            },
        }
    }

    fn float_ordering(l: Option<f64>, r: Option<f64>) -> Option<Option<std::cmp::Ordering>> {
        match (l, r) {
            (Some(l), Some(r)) => Some(Some(crate::float_semantics::compare_floats(l, r))),
            (None, None) => Some(None),
            _ => None,
        }
    }

    /// Every pair of edge values, under every operator, as two columns and as a column and a
    /// literal on either side, for Float64 and Float32.
    #[test]
    fn float_operands_follow_spark_ordering() -> Result<()> {
        let values = [
            Some(f64::NEG_INFINITY),
            Some(-1.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::MIN_POSITIVE),
            Some(f64::INFINITY),
            Some(f64::NAN),
            // A NaN with the sign bit set, as arithmetic produces on x86-64.
            Some(f64::from_bits(0xfff8_0000_0000_0000)),
            // A NaN with a payload.
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            None,
        ];
        // The Float32 pass computes Spark's answers from the values as Float32 holds them.
        let to_f32 = |v: Option<f64>| v.map(|v| v as f32 as f64);
        for float32 in [false, true] {
            let values: Vec<_> = if float32 {
                values.iter().map(|&v| to_f32(v)).collect()
            } else {
                values.to_vec()
            };
            let pairs: Vec<_> = values
                .iter()
                .flat_map(|&l| values.iter().map(move |&r| (l, r)))
                .collect();
            let left: Vec<_> = pairs.iter().map(|p| p.0).collect();
            let right: Vec<_> = pairs.iter().map(|p| p.1).collect();
            let array = |v: &[Option<f64>]| -> ArrayRef {
                if float32 {
                    Arc::new(arrow::array::Float32Array::from(
                        v.iter().map(|v| v.map(|v| v as f32)).collect::<Vec<_>>(),
                    ))
                } else {
                    Arc::new(Float64Array::from(v.to_vec()))
                }
            };
            let batch = batch(array(&left), array(&right));
            let schema = batch.schema();
            let (a, b) = columns();
            for op in COMPARISONS {
                let expr = spark_comparison(
                    Arc::clone(&a),
                    op,
                    Arc::clone(&b),
                    &schema,
                    FloatOperands::Normalize,
                )?;
                let expected: Vec<_> = pairs
                    .iter()
                    .map(|&(l, r)| spark_answer(op, float_ordering(l, r)))
                    .collect();
                assert_eq!(results(&expr, &batch), expected, "a {op} b");
            }
            // The first rows pair `values[0]` with each value in turn, so row `i` of column `b`
            // holds `values[i]`.
            for (i, &value) in values.iter().enumerate() {
                let lit = literal(batch.column(1), i);
                for op in COMPARISONS {
                    let column_first = spark_comparison(
                        Arc::clone(&a),
                        op,
                        Arc::clone(&lit),
                        &schema,
                        FloatOperands::Normalize,
                    )?;
                    let expected: Vec<_> = left
                        .iter()
                        .map(|&l| spark_answer(op, float_ordering(l, value)))
                        .collect();
                    assert_eq!(results(&column_first, &batch), expected, "a {op} {lit}");
                    let literal_first = spark_comparison(
                        Arc::clone(&lit),
                        op,
                        Arc::clone(&a),
                        &schema,
                        FloatOperands::Normalize,
                    )?;
                    let expected: Vec<_> = left
                        .iter()
                        .map(|&l| spark_answer(op, float_ordering(value, l)))
                        .collect();
                    assert_eq!(results(&literal_first, &batch), expected, "{lit} {op} a");
                }
            }
        }
        Ok(())
    }

    /// A literal operand is normalized while planning, so the comparison stays `column op
    /// literal`. With `FloatOperands::Raw`, as for a scan's data filters, the column and the
    /// literal of that shape are left as they are.
    #[test]
    fn float_operand_shapes() -> Result<()> {
        use crate::float_semantics::NormalizeNaNAndZero;
        let batch = batch(
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![1.0])),
        );
        let schema = batch.schema();
        let negative_zero: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Float64(Some(-0.0))));
        let expr = spark_comparison(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::clone(&negative_zero),
            &schema,
            FloatOperands::Normalize,
        )?;
        let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
        assert!(binary
            .left()
            .downcast_ref::<NormalizeNaNAndZero>()
            .is_some());
        let folded = binary.right().downcast_ref::<Literal>().unwrap();
        assert!(matches!(folded.value(), ScalarValue::Float64(Some(v)) if v.to_bits() == 0));

        // With `FloatOperands::Raw`, a float column compared with a literal keeps both as they
        // are, on either side.
        let expr = spark_comparison(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            Arc::clone(&negative_zero),
            &schema,
            FloatOperands::Raw,
        )?;
        let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
        assert!(binary.left().downcast_ref::<Column>().is_some());
        assert!(Arc::ptr_eq(binary.right(), &negative_zero));
        let expr = spark_comparison(
            Arc::clone(&negative_zero),
            Operator::Lt,
            Arc::new(Column::new("a", 0)),
            &schema,
            FloatOperands::Raw,
        )?;
        let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
        assert!(Arc::ptr_eq(binary.left(), &negative_zero));
        assert!(binary.right().downcast_ref::<Column>().is_some());

        // Any other shape is normalized even with `FloatOperands::Raw`, because a reader with
        // row-level pushdown drops the rows it rejects.
        let (a, b) = columns();
        let expr = spark_comparison(
            Arc::clone(&a),
            Operator::Lt,
            Arc::clone(&b),
            &schema,
            FloatOperands::Raw,
        )?;
        let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
        assert!(binary
            .left()
            .downcast_ref::<NormalizeNaNAndZero>()
            .is_some());
        assert!(binary
            .right()
            .downcast_ref::<NormalizeNaNAndZero>()
            .is_some());

        // Other operators are not comparisons and keep their operands.
        let expr = spark_comparison(
            Arc::clone(&a),
            Operator::Plus,
            Arc::clone(&b),
            &schema,
            FloatOperands::Normalize,
        )?;
        let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
        assert!(Arc::ptr_eq(binary.left(), &a));
        assert!(Arc::ptr_eq(binary.right(), &b));
        Ok(())
    }

    /// A bloom filter holds the bits of each value, so with `FloatOperands::Raw` an `=` against
    /// either zero probes the filter for both zeros, while other operators keep the literal. A
    /// NaN literal is normalized together with the column instead, because no single literal
    /// stands for every NaN a file can hold.
    #[test]
    fn raw_float_comparison_literals() -> Result<()> {
        use crate::float_semantics::NormalizeNaNAndZero;
        use datafusion::physical_expr::utils::{Guarantee, LiteralGuarantee};
        use std::collections::HashSet;
        let schema = Schema::new(vec![
            Field::new("f", DataType::Float32, true),
            Field::new("d", DataType::Float64, true),
        ]);
        let scalar = |value: f64, data_type: &DataType| match data_type {
            DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
            _ => ScalarValue::Float64(Some(value)),
        };
        for (index, name, data_type) in [(0, "f", DataType::Float32), (1, "d", DataType::Float64)] {
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(name, index));
            for zero in [-0.0, 0.0] {
                let literal: Arc<dyn PhysicalExpr> =
                    Arc::new(Literal::new(scalar(zero, &data_type)));
                for (left, right) in [(&column, &literal), (&literal, &column)] {
                    let expr = spark_comparison(
                        Arc::clone(left),
                        Operator::Eq,
                        Arc::clone(right),
                        &schema,
                        FloatOperands::Raw,
                    )?;
                    let guarantees = LiteralGuarantee::analyze(&expr);
                    assert_eq!(guarantees.len(), 1, "{expr}");
                    assert_eq!(guarantees[0].guarantee, Guarantee::In, "{expr}");
                    assert_eq!(guarantees[0].column.name(), name, "{expr}");
                    assert_eq!(
                        guarantees[0].literals,
                        HashSet::from([scalar(-0.0, &data_type), scalar(0.0, &data_type)]),
                        "{expr}"
                    );
                }
                let expr = spark_comparison(
                    Arc::clone(&column),
                    Operator::GtEq,
                    Arc::clone(&literal),
                    &schema,
                    FloatOperands::Raw,
                )?;
                let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
                assert!(Arc::ptr_eq(binary.left(), &column));
                assert!(Arc::ptr_eq(binary.right(), &literal));
            }
        }

        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("d", 1));
        for nan in [f64::NAN, f64::from_bits(0xfff8_0000_0000_0000)] {
            let expr = spark_comparison(
                Arc::clone(&column),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Float64(Some(nan)))),
                &schema,
                FloatOperands::Raw,
            )?;
            let binary = expr.downcast_ref::<BinaryExpr>().unwrap();
            assert!(binary
                .left()
                .downcast_ref::<NormalizeNaNAndZero>()
                .is_some());
            let folded = binary.right().downcast_ref::<Literal>().unwrap();
            assert!(
                matches!(folded.value(), ScalarValue::Float64(Some(v)) if v.to_bits() == f64::NAN.to_bits())
            );
        }
        Ok(())
    }

    /// Ordering and null-safe comparisons of lists and structs match `spark_comparator`, which
    /// compares float leaves in Spark's order without normalizing them.
    #[test]
    fn nested_float_operands_follow_spark_ordering() -> Result<()> {
        use crate::float_semantics::spark_comparator;
        let leaves = [
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(f64::from_bits(0xfff8_0000_0000_0000)),
            Some(f64::INFINITY),
            Some(1.0),
            None,
        ];
        // Lists of every pair of leaves, including lists of different lengths.
        let mut left_lists = vec![];
        let mut right_lists = vec![];
        for &l in &leaves {
            for &r in &leaves {
                left_lists.push(Some(vec![l]));
                right_lists.push(Some(vec![r]));
                left_lists.push(Some(vec![Some(1.0), l]));
                right_lists.push(Some(vec![Some(1.0), r, Some(2.0)]));
            }
        }
        left_lists.push(None);
        right_lists.push(Some(vec![]));
        left_lists.push(None);
        right_lists.push(None);
        let list = |lists: &[Option<Vec<Option<f64>>>]| -> ArrayRef {
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                lists.to_vec(),
            ))
        };
        let (left, right) = (list(&left_lists), list(&right_lists));
        let as_struct = |array: &ArrayRef| -> ArrayRef {
            Arc::new(StructArray::new(
                vec![Arc::new(Field::new("v", array.data_type().clone(), true))].into(),
                vec![Arc::clone(array)],
                None,
            ))
        };
        for (l, r) in [
            (Arc::clone(&left), Arc::clone(&right)),
            (as_struct(&left), as_struct(&right)),
        ] {
            let compare = spark_comparator(l.as_ref(), r.as_ref())?;
            let batch = batch(Arc::clone(&l), Arc::clone(&r));
            let (a, b) = columns();
            for op in COMPARISONS {
                let expr = spark_comparison(
                    Arc::clone(&a),
                    op,
                    Arc::clone(&b),
                    batch.schema().as_ref(),
                    FloatOperands::Normalize,
                )?;
                let expected: Vec<_> = (0..l.len())
                    .map(|i| {
                        let ordering = match (l.is_null(i), r.is_null(i)) {
                            (false, false) => Some(Some(compare(i, i))),
                            (true, true) => Some(None),
                            _ => None,
                        };
                        spark_answer(op, ordering)
                    })
                    .collect();
                assert_eq!(results(&expr, &batch), expected, "{} {op}", l.data_type());
            }
        }
        Ok(())
    }
}
