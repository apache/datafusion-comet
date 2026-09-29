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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::{cast::as_boolean_array, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{
    expressions::{CaseExpr, Column, Literal},
    PhysicalExpr,
};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// IfExpr is a wrapper around CaseExpr, because `IF(a, b, c)` is semantically equivalent to
/// `CASE WHEN a THEN b ELSE c END`.
#[derive(Debug, Eq)]
pub struct IfExpr {
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
    case_expr: Arc<CaseExpr>,
    column_literal: Option<ColumnLiteral>,
}

/// Only these branches can be read without evaluating expressions on unselected rows.
#[derive(Debug, Eq, PartialEq)]
struct ColumnLiteral {
    column_index: usize,
    literal: ScalarValue,
    data_type: DataType,
    literal_is_true: bool,
}

impl ColumnLiteral {
    fn new(
        column: &Arc<dyn PhysicalExpr>,
        literal: &Arc<dyn PhysicalExpr>,
        literal_is_true: bool,
    ) -> Option<Self> {
        let column = column.downcast_ref::<Column>()?;
        let literal = literal.downcast_ref::<Literal>()?.value();
        if literal.is_null() {
            return None;
        }
        Some(Self {
            column_index: column.index(),
            literal: literal.clone(),
            data_type: literal.data_type(),
            literal_is_true,
        })
    }
}

impl Hash for IfExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.if_expr.hash(state);
        self.true_expr.hash(state);
        self.false_expr.hash(state);
        self.case_expr.hash(state);
    }
}
impl PartialEq for IfExpr {
    fn eq(&self, other: &Self) -> bool {
        self.if_expr.eq(&other.if_expr)
            && self.true_expr.eq(&other.true_expr)
            && self.false_expr.eq(&other.false_expr)
            && self.case_expr.eq(&other.case_expr)
    }
}

impl std::fmt::Display for IfExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "If [if: {}, true_expr: {}, false_expr: {}]",
            self.if_expr, self.true_expr, self.false_expr
        )
    }
}

impl IfExpr {
    /// Create a new IF expression
    pub fn new(
        if_expr: Arc<dyn PhysicalExpr>,
        true_expr: Arc<dyn PhysicalExpr>,
        false_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let column_literal = ColumnLiteral::new(&true_expr, &false_expr, false)
            .or_else(|| ColumnLiteral::new(&false_expr, &true_expr, true));
        Self {
            column_literal,
            if_expr: Arc::clone(&if_expr),
            true_expr: Arc::clone(&true_expr),
            false_expr: Arc::clone(&false_expr),
            case_expr: Arc::new(
                CaseExpr::try_new(None, vec![(if_expr, true_expr)], Some(false_expr)).unwrap(),
            ),
        }
    }

    fn evaluate_column_literal(
        &self,
        batch: &RecordBatch,
        selection: &ColumnLiteral,
    ) -> Result<ColumnarValue> {
        // Check the input type before evaluating the condition. Falling back after that
        // could evaluate a nondeterministic condition twice. CASE retains responsibility
        // for coercion (including dictionary/value type differences).
        let Some(column) = batch
            .columns()
            .get(selection.column_index)
            .filter(|column| column.data_type() == &selection.data_type)
        else {
            return self.case_expr.evaluate(batch);
        };
        // As in CASE, expand scalar conditions to one row, then take the uniform path.
        let condition = self.if_expr.evaluate(batch)?.into_array(1)?;
        let condition = as_boolean_array(&condition).map_err(|error| {
            DataFusionError::Context(
                "WHEN expression did not return a BooleanArray".to_string(),
                Box::new(error),
            )
        })?;
        if condition.null_count() == 0 && !condition.has_false() {
            return Ok(if selection.literal_is_true {
                ColumnarValue::Scalar(selection.literal.clone())
            } else {
                ColumnarValue::Array(Arc::clone(column))
            });
        }
        if !condition.has_true() {
            return Ok(if selection.literal_is_true {
                ColumnarValue::Array(Arc::clone(column))
            } else {
                ColumnarValue::Scalar(selection.literal.clone())
            });
        }
        if condition.len() == column.len() {
            macro_rules! select {
                ($ty:ty, $value:expr) => {
                    return Ok(ColumnarValue::Array(select_primitive(
                        column.as_primitive::<$ty>(),
                        $value,
                        condition,
                        selection.literal_is_true,
                    )))
                };
            }
            match selection.literal {
                ScalarValue::Float64(Some(value)) => select!(Float64Type, value),
                ScalarValue::Float32(Some(value)) => select!(Float32Type, value),
                ScalarValue::Int64(Some(value)) => select!(Int64Type, value),
                ScalarValue::Int32(Some(value)) => select!(Int32Type, value),
                _ => {}
            }
        }
        // A single zip avoids filtering both branches and merging them back together.
        // Arrow treats NULL conditions as false, matching Spark's IF semantics.
        let literal = selection.literal.to_scalar()?;
        let result = if selection.literal_is_true {
            zip(condition, &literal, column)?
        } else {
            zip(condition, column, &literal)?
        };
        Ok(ColumnarValue::Array(result))
    }
}

impl PhysicalExpr for IfExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.true_expr.data_type(input_schema)?;
        Ok(data_type)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        if self.true_expr.nullable(_input_schema)? || self.false_expr.nullable(_input_schema)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match &self.column_literal {
            Some(selection) => self.evaluate_column_literal(batch, selection),
            None => self.case_expr.evaluate(batch),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.if_expr, &self.true_expr, &self.false_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IfExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            Arc::clone(&children[2]),
        )))
    }
}

fn select_primitive<T: ArrowPrimitiveType>(
    column: &PrimitiveArray<T>,
    literal: T::Native,
    condition: &BooleanArray,
    literal_is_true: bool,
) -> ArrayRef {
    // NULL conditions select the false branch. Normalize before inversion so a
    // NULL condition selects a false-branch literal, not a false-branch column.
    let when = match condition.nulls() {
        Some(nulls) => condition.values() & nulls.inner(),
        None => condition.values().clone(),
    };
    let selected: BooleanBuffer = if literal_is_true { when } else { !&when };
    let mut values = column.values().to_vec();
    for index in selected.set_indices() {
        values[index] = literal;
    }
    // The literal is non-NULL, including where it replaces a NULL column value.
    let nulls = column
        .nulls()
        .map(|nulls| NullBuffer::new(nulls.inner() | &selected));
    Arc::new(PrimitiveArray::<T>::new(values.into(), nulls))
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::{array::StringArray, datatypes::*};
    use datafusion::common::cast::as_int32_array;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    use super::*;

    /// Create an If expression
    fn if_fn(
        if_expr: Arc<dyn PhysicalExpr>,
        true_expr: Arc<dyn PhysicalExpr>,
        false_expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)))
    }

    #[test]
    fn test_if_1() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        let schema_ref = batch.schema();

        // if a = 'foo' 123 else 999
        let if_expr = binary(
            col("a", &schema_ref)?,
            Operator::Eq,
            lit("foo"),
            &schema_ref,
        )?;
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr);
        let result = expr?.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(999)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_if_2() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        let schema_ref = batch.schema();

        // if a >=1 123 else 999
        let if_expr = binary(col("a", &schema_ref)?, Operator::GtEq, lit(1), &schema_ref)?;
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr);
        let result = expr?.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(123)]);
        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_if_children() {
        let if_expr = lit(true);
        let true_expr = lit(123i32);
        let false_expr = lit(999i32);

        let expr = if_fn(if_expr, true_expr, false_expr).unwrap();
        let children = expr.children();
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].to_string(), "true");
        assert_eq!(children[1].to_string(), "123");
        assert_eq!(children[2].to_string(), "999");
    }
    /// Compare the specialized route with the pinned CASE implementation, including
    /// slices whose validity and value bitmaps do not start on a word boundary.
    #[test]
    fn test_if_column_literal_slices() -> Result<()> {
        use arrow::array::{
            ArrayRef, BooleanArray, DictionaryArray, Float32Array, Float64Array, Int64Array,
        };
        use datafusion::common::ScalarValue;

        let values: Vec<Option<f64>> = (0..200)
            .map(|i| match i % 7 {
                0 => None,
                1 => Some(-0.0),
                2 => Some(f64::from_bits(0x7ff8_0000_0000_0042)),
                3 => Some(f64::INFINITY),
                _ => Some(i as f64),
            })
            .collect();
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter((0..200).map(|i| (i % 5 != 0).then_some(i % 3))),
            Arc::new(StringArray::from(vec![Some(""), None, Some("中文")])),
        )?;
        let cases: Vec<(ArrayRef, ScalarValue)> = vec![
            (
                Arc::new(Float64Array::from(values.clone())),
                ScalarValue::Float64(Some(-0.0)),
            ),
            (
                Arc::new(Float64Array::from(values)),
                ScalarValue::Float64(Some(f64::from_bits(0x7ff8_0000_0000_0099))),
            ),
            (
                Arc::new(Float32Array::from_iter((0..200).map(|i| match i % 7 {
                    0 => None,
                    1 => Some(-0.0),
                    2 => Some(f32::from_bits(0x7fc0_0042)),
                    3 => Some(f32::NEG_INFINITY),
                    _ => Some(i as f32),
                }))),
                ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0099))),
            ),
            (
                Arc::new(Int32Array::from_iter((0..200).map(|i| match i % 4 {
                    0 => None,
                    1 => Some(i32::MIN),
                    2 => Some(i32::MAX),
                    _ => Some(i),
                }))),
                ScalarValue::Int32(Some(-1)),
            ),
            (
                Arc::new(Int64Array::from_iter(
                    (0..200).map(|i| (i % 7 != 0).then_some(i as i64)),
                )),
                ScalarValue::Int64(Some(-1)),
            ),
            (
                Arc::new(StringArray::from_iter((0..200).map(|i| {
                    (i % 7 != 0).then_some(if i % 2 == 0 { "" } else { "中文" })
                }))),
                ScalarValue::Utf8(Some("replacement".to_string())),
            ),
            (
                Arc::new(dictionary),
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("replacement".to_string()))),
                ),
            ),
        ];
        for (array, scalar) in cases {
            for mask in [
                BooleanArray::from(vec![true; 200]),
                BooleanArray::from(vec![false; 200]),
                BooleanArray::from(vec![None; 200]),
                BooleanArray::from_iter((0..200).map(|i| match i % 3 {
                    0 => Some(true),
                    1 => Some(false),
                    _ => None,
                })),
                // A null condition can have a true value bit underneath its validity bit.
                BooleanArray::new(
                    BooleanBuffer::new_set(200),
                    Some(NullBuffer::from_iter((0..200).map(|i| i % 3 == 0))),
                ),
            ] {
                let batch = RecordBatch::try_from_iter(vec![
                    ("value", Arc::clone(&array)),
                    ("mask", Arc::new(mask) as ArrayRef),
                ])?;
                for offset in [0, 1, 7, 63] {
                    for rows in [0, 1, 63, 64, 65, 129] {
                        let batch = batch.slice(offset, rows);
                        for literal_is_true in [false, true] {
                            let column = col("value", batch.schema_ref())?;
                            let literal =
                                Arc::new(Literal::new(scalar.clone())) as Arc<dyn PhysicalExpr>;
                            let (t, f) = if literal_is_true {
                                (literal, column)
                            } else {
                                (column, literal)
                            };
                            let predicate = col("mask", batch.schema_ref())?;
                            let reference = CaseExpr::try_new(
                                None,
                                vec![(Arc::clone(&predicate), Arc::clone(&t))],
                                Some(Arc::clone(&f)),
                            )?;
                            let expr = IfExpr::new(predicate, t, f);
                            assert!(expr.column_literal.is_some());
                            let expected = reference.evaluate(&batch)?.into_array(rows)?;
                            let actual = expr.evaluate(&batch)?.into_array(rows)?;
                            assert_eq!(
                                expected.to_data(),
                                actual.to_data(),
                                "{} offset={offset} rows={rows} literal_is_true={literal_is_true}",
                                array.data_type()
                            );
                            // Preserve NaN payloads and the sign of zero, not just SQL equality.
                            for row in 0..rows {
                                if actual.is_null(row) {
                                    continue;
                                }
                                match array.data_type() {
                                    DataType::Float64 => assert_eq!(
                                        expected.as_primitive::<Float64Type>().value(row).to_bits(),
                                        actual.as_primitive::<Float64Type>().value(row).to_bits(),
                                    ),
                                    DataType::Float32 => assert_eq!(
                                        expected.as_primitive::<Float32Type>().value(row).to_bits(),
                                        actual.as_primitive::<Float32Type>().value(row).to_bits(),
                                    ),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_if_type_coercion_and_scalar_conditions() -> Result<()> {
        use arrow::array::{ArrayRef, BooleanArray};
        use datafusion::common::ScalarValue;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "value",
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            ),
            (
                "mask",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            ),
        ])?;
        for predicate in [
            col("mask", batch.schema_ref())?,
            lit(true),
            lit(false),
            Arc::new(Literal::new(ScalarValue::Boolean(None))),
        ] {
            for literal in [lit(7_i32), lit(7_i64), lit("9")] {
                for literal_is_true in [false, true] {
                    let column = col("value", batch.schema_ref())?;
                    let (t, f) = if literal_is_true {
                        (Arc::clone(&literal), column)
                    } else {
                        (column, Arc::clone(&literal))
                    };
                    let reference = CaseExpr::try_new(
                        None,
                        vec![(Arc::clone(&predicate), Arc::clone(&t))],
                        Some(Arc::clone(&f)),
                    )?;
                    let expr = IfExpr::new(Arc::clone(&predicate), t, f);
                    assert_eq!(
                        reference.evaluate(&batch)?.into_array(3)?.to_data(),
                        expr.evaluate(&batch)?.into_array(3)?.to_data()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_if_computed_branches_remain_lazy() -> Result<()> {
        use arrow::array::{ArrayRef, BooleanArray};
        use arrow::compute::CastOptions;
        use datafusion::physical_expr::expressions::CastExpr;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "value",
                Arc::new(StringArray::from(vec!["7", "bad", "bad"])) as ArrayRef,
            ),
            (
                "mask",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            ),
        ])?;
        let cast = Arc::new(CastExpr::new(
            col("value", batch.schema_ref())?,
            DataType::Int32,
            Some(CastOptions {
                safe: false,
                ..Default::default()
            }),
        )) as Arc<dyn PhysicalExpr>;
        let expr = IfExpr::new(
            col("mask", batch.schema_ref())?,
            Arc::clone(&cast),
            lit(0_i32),
        );
        assert!(expr.column_literal.is_none());
        assert_eq!(
            expr.evaluate(&batch)?.into_array(3)?.as_ref(),
            &Int32Array::from(vec![7, 0, 0]) as &dyn arrow::array::Array
        );
        // Replacing children must recompute eligibility and preserve the required error.
        let rebuilt = Arc::new(IfExpr::new(
            col("mask", batch.schema_ref())?,
            col("value", batch.schema_ref())?,
            lit("ok"),
        ))
        .with_new_children(vec![col("mask", batch.schema_ref())?, lit(0_i32), cast])?;
        assert!(rebuilt.evaluate(&batch).is_err());
        Ok(())
    }
}
