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

use crate::{Cast, EvalMode, SparkCastOptions};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::Result;
use datafusion::logical_expr::type_coercion::binary::type_union_coercion;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{expressions::CaseExpr, PhysicalExpr};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// Creates an `IF`, casting a branch whose type differs from the other's to their common type.
///
/// Spark adds no cast when the branches differ only in whether a nested field can be NULL, or in
/// the case of a struct field name. But [`IfExpr`] reports the THEN branch's type, and returns the
/// ELSE branch's array unchanged when no row of a batch takes the THEN branch.
pub fn create_if_expr(
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let true_type = true_expr.data_type(input_schema)?;
    let false_type = false_expr.data_type(input_schema)?;
    let Some(common_type) = if_common_type(&true_type, &false_type) else {
        return Ok(Arc::new(IfExpr::new(if_expr, true_expr, false_expr)));
    };
    Ok(Arc::new(IfExpr::new(
        if_expr,
        coerce_branch(true_expr, &true_type, &common_type),
        coerce_branch(false_expr, &false_type, &common_type),
    )))
}

/// Reconciles Spark IF branches positionally, retaining THEN names and merging nullability.
/// Spark has already coerced the branches to the same SQL type. DataFusion's struct union may
/// instead match by name, pairing different positions when names differ only in case.
fn if_common_type(then_type: &DataType, else_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::FieldRef;

    fn field(then_field: &FieldRef, else_field: &FieldRef) -> Option<FieldRef> {
        Some(Arc::new(
            then_field
                .as_ref()
                .clone()
                .with_data_type(if_common_type(
                    then_field.data_type(),
                    else_field.data_type(),
                )?)
                .with_nullable(then_field.is_nullable() || else_field.is_nullable()),
        ))
    }

    match (then_type, else_type) {
        (DataType::Struct(then_fields), DataType::Struct(else_fields)) => {
            if then_fields.len() != else_fields.len() {
                return None;
            }
            Some(DataType::Struct(
                then_fields
                    .iter()
                    .zip(else_fields)
                    .map(|(t, e)| field(t, e))
                    .collect::<Option<Vec<_>>>()?
                    .into(),
            ))
        }
        (DataType::List(t), DataType::List(e)) => Some(DataType::List(field(t, e)?)),
        (DataType::Map(t, t_sorted), DataType::Map(e, e_sorted)) => {
            Some(DataType::Map(field(t, e)?, *t_sorted && *e_sorted))
        }
        _ => type_union_coercion(then_type, else_type),
    }
}

/// Casts an IF branch whose type is `data_type` to the branches' `common_type`.
///
/// The branches share a Spark type, so any difference is in the Arrow representation. For a
/// timestamp that is the timezone label, and the cast only relabels it, but Comet's cast still
/// needs a timezone. Every `TimestampType` value in a native plan is labelled UTC.
fn coerce_branch(
    expr: Arc<dyn PhysicalExpr>,
    data_type: &DataType,
    common_type: &DataType,
) -> Arc<dyn PhysicalExpr> {
    // A branch that already has the common type is not wrapped in a cast, which would do nothing
    // but hide what the branch is from the evaluation.
    if data_type == common_type {
        return expr;
    }
    let cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    Arc::new(Cast::new(
        expr,
        common_type.clone(),
        cast_options,
        None,
        None,
    ))
}

/// IfExpr is a wrapper around CaseExpr, because `IF(a, b, c)` is semantically equivalent to
/// `CASE WHEN a THEN b ELSE c END`.
#[derive(Debug, Eq)]
pub struct IfExpr {
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
    // we delegate to case_expr for evaluation
    case_expr: Arc<CaseExpr>,
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
        Self {
            if_expr: Arc::clone(&if_expr),
            true_expr: Arc::clone(&true_expr),
            false_expr: Arc::clone(&false_expr),
            case_expr: Arc::new(
                CaseExpr::try_new(None, vec![(if_expr, true_expr)], Some(false_expr)).unwrap(),
            ),
        }
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
        self.case_expr.evaluate(batch)
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

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, AsArray, BooleanArray, Int32Array, StructArray, TimestampMicrosecondArray,
    };
    use arrow::{array::StringArray, datatypes::*};
    use datafusion::common::cast::as_int32_array;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    use super::*;

    /// Evaluates an IF over `batch`, whose first row takes the THEN branch and second row the ELSE
    /// branch, and over each row alone, and checks that every result has the type the IF reports.
    /// Returns the result for the whole batch.
    fn evaluate_if(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> ArrayRef {
        let data_type = expr.data_type(&batch.schema()).unwrap();
        // Both branches, only THEN, and only ELSE, whose array `IfExpr` returns unchanged
        for (offset, len) in [(0, 2), (0, 1), (1, 1)] {
            let result = expr.evaluate(&batch.slice(offset, len)).unwrap();
            let result = result.into_array(len).unwrap();
            assert_eq!(
                result.data_type(),
                &data_type,
                "{expr} over rows {offset}..+{len}"
            );
        }
        expr.evaluate(batch).unwrap().into_array(2).unwrap()
    }

    /// Spark matches struct fields by position even when their case-distinct names are reordered.
    /// Name-based union pairs the first INT with the second DOUBLE and silently widens it.
    #[test]
    fn if_reconciles_case_variant_fields_positionally() {
        use arrow::array::Float64Array;

        let then_fields: arrow::datatypes::Fields = vec![
            Field::new("x", DataType::Int32, true),
            Field::new("X", DataType::Float64, false),
        ]
        .into();
        let else_fields: arrow::datatypes::Fields = vec![
            Field::new("X", DataType::Int32, false),
            Field::new("x", DataType::Float64, true),
        ]
        .into();
        let expected_fields: arrow::datatypes::Fields = vec![
            Field::new("x", DataType::Int32, true),
            Field::new("X", DataType::Float64, true),
        ]
        .into();
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("t", DataType::Struct(then_fields.clone()), false),
            Field::new("e", DataType::Struct(else_fields.clone()), false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(StructArray::new(
                    then_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![Some(7), None])),
                        Arc::new(Float64Array::from(vec![5.5, 5.5])),
                    ],
                    None,
                )),
                Arc::new(StructArray::new(
                    else_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![0, 0])),
                        Arc::new(Float64Array::from(vec![Some(9.5), None])),
                    ],
                    None,
                )),
            ],
        )
        .unwrap();
        let c = |name: &str| col(name, &schema).unwrap();
        let expr = create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap();
        assert_eq!(
            expr.data_type(&schema).unwrap(),
            DataType::Struct(expected_fields.clone())
        );
        let result = evaluate_if(&expr, &batch);
        let expected = StructArray::new(
            expected_fields,
            vec![
                Arc::new(Int32Array::from(vec![7, 0])),
                Arc::new(Float64Array::from(vec![Some(5.5), None])),
            ],
            None,
        );
        assert_eq!(result.as_ref(), &expected);
    }

    /// IF branches that share a Spark timestamp type but carry different Arrow timezone labels are
    /// reconciled by relabelling one of them, as CASE branches are. The result has the THEN
    /// branch's label, which `IfExpr` reported before the ELSE branch was cast, unless it has none.
    /// Every case ends up labelled UTC, like every `TimestampType` value in a native plan.
    #[test]
    fn if_reconciles_timestamp_timezone_labels() {
        let timestamp =
            |label: Option<&str>| DataType::Timestamp(TimeUnit::Microsecond, label.map(Into::into));
        // The THEN branch's label, the ELSE branch's label, and the result's
        for (then_label, else_label, label) in [
            (None, Some("UTC"), Some("UTC")),
            (Some("UTC"), Some("Etc/UTC"), Some("UTC")),
            (Some("UTC"), None, Some("UTC")),
        ] {
            let schema = Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("t", timestamp(then_label), true),
                Field::new("e", timestamp(else_label), true),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(BooleanArray::from(vec![true, false])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![1, 2]).with_timezone_opt(then_label),
                    ),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![10, 20]).with_timezone_opt(else_label),
                    ),
                ],
            )
            .unwrap();
            let c = |name: &str| col(name, &schema).unwrap();
            let expr = create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap();
            let labels = format!("{then_label:?} {else_label:?}");
            assert_eq!(
                expr.data_type(&schema).unwrap(),
                timestamp(label),
                "{labels}"
            );
            let result = evaluate_if(&expr, &batch);
            let values = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(values.values().to_vec(), vec![1, 20], "{labels}");
        }
    }

    /// IF branches whose struct field differs only in whether it can be NULL, or in the case of its
    /// name, which Spark adds no cast for. The result's field can be NULL if either branch's can,
    /// and has the THEN branch's name, as in Spark's `If.dataType`.
    #[test]
    fn if_reconciles_struct_field_nullability_and_names() {
        let field =
            |name: &str, nullable: bool| Arc::new(Field::new(name, DataType::Int32, nullable));
        let struct_type = |field: &FieldRef| DataType::Struct(vec![Arc::clone(field)].into());
        let column = |field: &FieldRef, values: Vec<Option<i32>>| -> ArrayRef {
            Arc::new(StructArray::new(
                vec![Arc::clone(field)].into(),
                vec![Arc::new(Int32Array::from(values)) as ArrayRef],
                None,
            ))
        };
        // The THEN branch's field, the ELSE branch's field, and the result's
        for (then_field, else_field, result_field) in [
            (field("x", false), field("x", true), field("x", true)),
            (field("x", true), field("x", false), field("x", true)),
            (field("x", true), field("X", true), field("x", true)),
            (field("X", false), field("x", true), field("X", true)),
        ] {
            let schema = Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("t", struct_type(&then_field), true),
                Field::new("e", struct_type(&else_field), true),
            ]);
            // A NULL field in the row that takes the ELSE branch, where it can be NULL
            let else_values = if else_field.is_nullable() {
                vec![Some(10), None]
            } else {
                vec![Some(10), Some(20)]
            };
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(BooleanArray::from(vec![true, false])),
                    column(&then_field, vec![Some(1), Some(2)]),
                    column(&else_field, else_values.clone()),
                ],
            )
            .unwrap();
            let c = |name: &str| col(name, &schema).unwrap();
            let expr = create_if_expr(c("b"), c("t"), c("e"), &schema).unwrap();
            let fields = format!("{then_field:?} {else_field:?}");
            assert_eq!(
                expr.data_type(&schema).unwrap(),
                struct_type(&result_field),
                "{fields}"
            );
            let result = evaluate_if(&expr, &batch);
            let expected = column(&result_field, vec![Some(1), else_values[1]]);
            assert_eq!(result.as_ref(), expected.as_ref(), "{fields}");
        }
    }

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
}
