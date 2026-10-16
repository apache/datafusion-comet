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

use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::CaseExpr;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// Spark's `CASE WHEN`, which Comet also uses for `COALESCE`.
///
/// It evaluates through DataFusion's [`CaseExpr`], except that it returns an empty array for an
/// empty batch. `CaseExpr` can return a scalar there, such as a NULL literal in a branch, even
/// when the CASE reads a column, and a scalar from an empty batch is taken to mean that the
/// expression is a constant, as IN does to build its list once.
#[derive(Debug, Eq)]
pub struct SparkCaseExpr {
    case_expr: Arc<dyn PhysicalExpr>,
}

impl SparkCaseExpr {
    pub fn new(case_expr: CaseExpr) -> Self {
        Self {
            case_expr: Arc::new(case_expr),
        }
    }
}

impl Hash for SparkCaseExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.case_expr.hash(state);
    }
}

impl PartialEq for SparkCaseExpr {
    fn eq(&self, other: &Self) -> bool {
        self.case_expr.eq(&other.case_expr)
    }
}

impl Display for SparkCaseExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.case_expr, f)
    }
}

impl PhysicalExpr for SparkCaseExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.case_expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.case_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if batch.num_rows() == 0 {
            let data_type = self.data_type(&batch.schema())?;
            return Ok(ColumnarValue::Array(new_empty_array(&data_type)));
        }
        self.case_expr.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.case_expr.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let case_expr = Arc::clone(&self.case_expr).with_new_children(children)?;
        if !case_expr.is::<CaseExpr>() {
            return internal_err!("CaseExpr returned {case_expr} for new children");
        }
        Ok(Arc::new(Self { case_expr }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.case_expr.fmt_sql(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IfExpr;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{col, lit, BinaryExpr};

    /// A scalar from an empty batch is taken to mean the expression is constant, so a CASE or IF
    /// that reads a column has to return an empty array.
    #[test]
    fn empty_batch_returns_an_empty_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();
        let a = col("a", &schema).unwrap();
        let b = col("b", &schema).unwrap();
        let a_is_1: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::Eq, lit(1i64)));
        let null = lit(ScalarValue::Int64(None));
        // CaseExpr returns the NULL literal as a scalar for an empty batch
        let case: Arc<dyn PhysicalExpr> = Arc::new(SparkCaseExpr::new(
            CaseExpr::try_new(
                None,
                vec![(Arc::clone(&a_is_1), Arc::clone(&null))],
                Some(b),
            )
            .unwrap(),
        ));
        // IF(a = 1, NULL, a), as nullif(a, 1) is planned
        let if_expr: Arc<dyn PhysicalExpr> = Arc::new(IfExpr::new(a_is_1, null, a));
        for expr in [case, if_expr] {
            match expr.evaluate(&batch).unwrap() {
                ColumnarValue::Array(array) => {
                    assert_eq!(array.len(), 0, "{expr}");
                    assert_eq!(array.data_type(), &DataType::Int64, "{expr}");
                }
                other => panic!("{expr} returned {other:?} for an empty batch"),
            }
        }
    }

    #[test]
    fn with_new_children_keeps_the_wrapper() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = col("a", &schema).unwrap();
        let a_is_1: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::Eq, lit(1i64)));
        let case: Arc<dyn PhysicalExpr> = Arc::new(SparkCaseExpr::new(
            CaseExpr::try_new(None, vec![(a_is_1, lit(ScalarValue::Int64(None)))], Some(a))
                .unwrap(),
        ));
        let children = case.children().into_iter().cloned().collect();
        let rebuilt = Arc::clone(&case).with_new_children(children).unwrap();
        assert!(rebuilt.is::<SparkCaseExpr>());
        assert_eq!(&rebuilt, &case);
    }
}
