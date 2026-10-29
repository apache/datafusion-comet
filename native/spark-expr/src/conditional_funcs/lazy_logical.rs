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

use super::case_when::cannot_fail;
use super::null_short_circuit::project;
use crate::{spark_comparison, FloatOperands};
use arrow::array::{Array, BooleanArray};
use arrow::compute::{and_kleene, not, or_kleene};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, NotExpr};
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Spark's `AND` and `OR`, which evaluate the right operand only for the rows that the left one
/// has not decided: the rows where it is not false for `AND`, and not true for `OR`.
///
/// DataFusion's `BinaryExpr` evaluates both operands over the whole batch, so a right operand that
/// can fail, such as an ANSI division or cast, would fail on a row that Spark never evaluates it
/// for, such as `v != 0 AND 100 / v > 1` on a row where `v = 0`.
#[derive(Debug)]
pub struct LazyLogical {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    /// The input columns that `right` reads
    projection: Vec<usize>,
    /// `right` reading those columns from a batch of just them, so that skipping rows filters no
    /// other column. It is evaluated in place of `right` for every batch, since rebuilding it gave
    /// it its own copy of any state kept across batches
    projected: Arc<dyn PhysicalExpr>,
}

impl LazyLogical {
    /// Creates `left op right` for an `AND` or `OR` over Boolean operands. It evaluates `right`
    /// lazily if `right` could fail on a row that `left` decides, and is a plain [`BinaryExpr`]
    /// otherwise.
    pub fn wrap(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let booleans = matches!(op, Operator::And | Operator::Or)
            && left.data_type(input_schema).ok() == Some(DataType::Boolean)
            && right.data_type(input_schema).ok() == Some(DataType::Boolean);
        if !booleans || cannot_fail(&right, input_schema) {
            return Ok(Arc::new(BinaryExpr::new(left, op, right)));
        }
        Ok(Arc::new(Self::try_new(left, op, right)?))
    }

    fn try_new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        let (projection, projected) = project(&right)?;
        Ok(Self {
            left,
            op,
            right,
            projection,
            projected,
        })
    }
}

/// Creates `value IN (candidates)` as the chain of equalities that Spark evaluates it as, each
/// candidate only for the rows that no earlier one has matched, or `None` if it has no such form.
///
/// Spark's `In` returns true at the first candidate that equals the value, and NULL if none does
/// and one is NULL, which is that of `value = c1 OR value = c2 OR ...`. A candidate that can fail
/// is then evaluated only for the rows that the ones before it have not matched. The value is
/// evaluated once per candidate, so this requires it to be a cheap expression that cannot fail.
/// This returns `None` when no candidate after the first can fail, when the value cannot be
/// evaluated again, and when a candidate has another type than the value.
pub fn lazy_in_list(
    value: &Arc<dyn PhysicalExpr>,
    candidates: &[Arc<dyn PhysicalExpr>],
    negated: bool,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    if candidates.len() < 2
        || candidates[1..].iter().all(|c| cannot_fail(c, schema))
        || !cannot_fail(value, schema)
    {
        return Ok(None);
    }
    let value_type = value.data_type(schema)?;
    for candidate in candidates {
        if candidate.data_type(schema)? != value_type {
            return Ok(None);
        }
    }
    let mut chain: Option<Arc<dyn PhysicalExpr>> = None;
    for candidate in candidates {
        let equal = spark_comparison(
            Arc::clone(value),
            Operator::Eq,
            Arc::clone(candidate),
            schema,
            FloatOperands::Normalize,
        )?;
        chain = Some(match chain {
            Some(matched) => LazyLogical::wrap(matched, Operator::Or, equal, schema)?,
            None => equal,
        });
    }
    let chain = chain.expect("candidates is not empty");
    Ok(Some(if negated {
        Arc::new(NotExpr::new(chain))
    } else {
        chain
    }))
}

/// The rows where `array` is true, with no NULL
fn is_true(array: &BooleanArray) -> BooleanArray {
    match array.nulls() {
        Some(nulls) => BooleanArray::new(array.values() & nulls.inner(), None),
        None => BooleanArray::new(array.values().clone(), None),
    }
}

impl Hash for LazyLogical {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.op.hash(state);
        self.right.hash(state);
    }
}

impl PartialEq for LazyLogical {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.op == other.op && self.right.eq(&other.right)
    }
}

impl Eq for LazyLogical {}

impl Display for LazyLogical {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for LazyLogical {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let left = self.left.evaluate(batch)?.into_array(num_rows)?;
        let left = as_boolean_array(&left)?;
        // The rows that the left operand decides: false for AND, true for OR
        let decided = match self.op {
            Operator::And => is_true(&not(left)?),
            _ => is_true(left),
        };
        let pending = not(&decided)?;
        let pending_rows = pending.true_count();
        if pending_rows == 0 {
            // No row needs the right operand, and the left one is the result on every row
            return Ok(ColumnarValue::Array(Arc::new(left.clone())));
        }
        let args = batch.project(&self.projection)?;
        let right = if pending_rows == num_rows {
            self.projected.evaluate(&args)?
        } else {
            self.projected.evaluate_selection(&args, &pending)?
        }
        .into_array(num_rows)?;
        let right = as_boolean_array(&right)?;
        // The right operand is NULL on the rows it was skipped for, which the left one decides
        let result: BooleanArray = match self.op {
            Operator::And => and_kleene(left, right)?,
            _ => or_kleene(left, right)?,
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [left, right] = <[_; 2]>::try_from(children).map_err(|children| {
            internal_datafusion_err!("LazyLogical expects 2 children, got {}", children.len())
        })?;
        Ok(Arc::new(Self::try_new(left, self.op, right)?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        self.left.fmt_sql(f)?;
        write!(f, " {} ", self.op)?;
        self.right.fmt_sql(f)?;
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cast, EvalMode, SparkCastOptions};
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{col, lit, Literal};

    fn batch(v: Vec<Option<i32>>, s: Vec<&str>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("v", DataType::Int32, true),
            Field::new("s", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(v)),
                Arc::new(StringArray::from(s)),
            ],
        )
        .unwrap()
    }

    /// `CAST(s AS INT) > 1` under ANSI, which fails on a string that is not an integer
    fn parsed_gt_one(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let cast = Arc::new(Cast::new(
            col("s", schema).unwrap(),
            DataType::Int32,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi),
            None,
            None,
        ));
        Arc::new(BinaryExpr::new(cast, Operator::Gt, lit(1)))
    }

    fn v_cmp(schema: &Schema, op: Operator, value: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col("v", schema).unwrap(), op, lit(value)))
    }

    fn evaluate(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<Vec<Option<bool>>> {
        let array: ArrayRef = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(as_boolean_array(&array)?.iter().collect())
    }

    #[test]
    fn and_skips_rows_the_left_operand_decides() -> Result<()> {
        let batch = batch(vec![Some(0), Some(1), Some(0)], vec!["bad", "5", "worse"]);
        let schema = batch.schema();
        // BinaryExpr fails on the 'bad' of a row with v = 0
        let eager: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            v_cmp(&schema, Operator::NotEq, 0),
            Operator::And,
            parsed_gt_one(&schema),
        ));
        assert!(evaluate(&eager, &batch).is_err());

        let lazy = LazyLogical::wrap(
            v_cmp(&schema, Operator::NotEq, 0),
            Operator::And,
            parsed_gt_one(&schema),
            &schema,
        )?;
        assert!(lazy.is::<LazyLogical>());
        assert_eq!(
            evaluate(&lazy, &batch)?,
            vec![Some(false), Some(true), Some(false)]
        );
        Ok(())
    }

    #[test]
    fn or_skips_rows_the_left_operand_decides() -> Result<()> {
        let batch = batch(vec![Some(0), Some(1), Some(0)], vec!["bad", "5", "worse"]);
        let schema = batch.schema();
        let lazy = LazyLogical::wrap(
            v_cmp(&schema, Operator::Eq, 0),
            Operator::Or,
            parsed_gt_one(&schema),
            &schema,
        )?;
        assert_eq!(
            evaluate(&lazy, &batch)?,
            vec![Some(true), Some(true), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn null_left_operand_evaluates_the_right_one() -> Result<()> {
        let failing = batch(vec![None], vec!["bad"]);
        let batch = batch(vec![None, Some(0), None], vec!["5", "bad", "0"]);
        let schema = batch.schema();
        let and = LazyLogical::wrap(
            v_cmp(&schema, Operator::NotEq, 0),
            Operator::And,
            parsed_gt_one(&schema),
            &schema,
        )?;
        // NULL AND true is NULL, false AND (not evaluated) is false, NULL AND false is false
        assert_eq!(
            evaluate(&and, &batch)?,
            vec![None, Some(false), Some(false)]
        );
        // A row with a NULL left operand is evaluated, so its right operand can fail
        assert!(evaluate(&and, &failing).is_err());
        Ok(())
    }

    #[test]
    fn evaluates_every_row_when_none_is_decided() -> Result<()> {
        let batch = batch(vec![Some(1), Some(2)], vec!["5", "0"]);
        let schema = batch.schema();
        let lazy = LazyLogical::wrap(
            v_cmp(&schema, Operator::NotEq, 0),
            Operator::And,
            parsed_gt_one(&schema),
            &schema,
        )?;
        assert_eq!(evaluate(&lazy, &batch)?, vec![Some(true), Some(false)]);
        // An empty batch evaluates nothing
        assert!(evaluate(&lazy, &batch.slice(0, 0))?.is_empty());
        Ok(())
    }

    #[test]
    fn infallible_right_operand_stays_a_binary_expr() -> Result<()> {
        let batch = batch(vec![Some(1)], vec!["5"]);
        let schema = batch.schema();
        let expr = LazyLogical::wrap(
            v_cmp(&schema, Operator::NotEq, 0),
            Operator::And,
            v_cmp(&schema, Operator::Lt, 5),
            &schema,
        )?;
        assert!(expr.is::<BinaryExpr>());
        Ok(())
    }

    #[test]
    fn scalar_left_operand() -> Result<()> {
        let batch = batch(vec![Some(1), None], vec!["bad", "worse"]);
        let schema = batch.schema();
        let lazy = LazyLogical::wrap(
            Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
            Operator::And,
            parsed_gt_one(&schema),
            &schema,
        )?;
        assert_eq!(evaluate(&lazy, &batch)?, vec![Some(false); 2]);
        Ok(())
    }

    fn parsed(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(Cast::new(
            col("s", schema).unwrap(),
            DataType::Int32,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi),
            None,
            None,
        ))
    }

    #[test]
    fn in_list_evaluates_a_candidate_for_unmatched_rows() -> Result<()> {
        // v IN (0, CAST(s AS INT)): the second candidate is skipped where v = 0
        let batch = batch(
            vec![Some(0), Some(5), None, Some(7)],
            vec!["bad", "5", "1", "8"],
        );
        let schema = batch.schema();
        let candidates = vec![lit(0), parsed(&schema)];
        let value = col("v", &schema)?;
        let expr = crate::spark_in_list(Arc::clone(&value), candidates.clone(), false, &schema)?;
        assert_eq!(
            evaluate(&expr, &batch)?,
            vec![Some(true), Some(true), None, Some(false)]
        );
        let expr = crate::spark_in_list(value, candidates, true, &schema)?;
        assert_eq!(
            evaluate(&expr, &batch)?,
            vec![Some(false), Some(false), None, Some(true)]
        );
        Ok(())
    }
}
