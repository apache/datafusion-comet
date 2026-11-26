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

use crate::CaseWhenExpr;
use arrow::{
    datatypes::{DataType, Field, FieldRef, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// IfExpr is a wrapper around CaseWhenExpr, because `IF(a, b, c)` is semantically equivalent to
/// `CASE WHEN a THEN b ELSE c END`.
#[derive(Debug, Eq)]
pub struct IfExpr {
    if_expr: Arc<dyn PhysicalExpr>,
    true_expr: Arc<dyn PhysicalExpr>,
    false_expr: Arc<dyn PhysicalExpr>,
    // we delegate to case_expr for evaluation
    case_expr: Arc<CaseWhenExpr>,
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
                CaseWhenExpr::try_new(vec![(if_expr, true_expr)], Some(false_expr)).unwrap(),
            ),
        }
    }

    /// The `CASE WHEN` that this evaluates as.
    pub(crate) fn case_when(&self) -> &CaseWhenExpr {
        &self.case_expr
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

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let field = Field::new(
            format!("{self}"),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        );
        Ok(Arc::new(
            self.case_expr
                .branch_metadata(input_schema)?
                .add_to_field(field),
        ))
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
    use arrow::array::Int32Array;
    use arrow::{array::StringArray, datatypes::*};
    use datafusion::common::cast::as_int32_array;
    use datafusion::common::{DataFusionError, ScalarValue};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    use super::*;

    const EXTENSION_TYPE_NAME_KEY: &str = "ARROW:extension:name";

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
    fn return_field_keeps_branch_metadata() -> Result<()> {
        let tagged = |name: &str, extension_name: &str| {
            Field::new(name, DataType::Binary, true)
                .with_metadata([(EXTENSION_TYPE_NAME_KEY, extension_name)])
        };
        let schema = Schema::new(vec![
            Field::new("p", DataType::Boolean, true),
            tagged("g1", "geoarrow.wkb"),
            tagged("g2", "geoarrow.wkb"),
            tagged("j", "arrow.json"),
        ]);
        let c = |name: &str| col(name, &schema).unwrap();
        fn extension_name(field: &Field) -> Option<&str> {
            field
                .metadata()
                .get(EXTENSION_TYPE_NAME_KEY)
                .map(String::as_str)
        }

        // Both branches carry the extension type
        let expr = IfExpr::new(c("p"), c("g1"), c("g2"));
        let field = expr.return_field(&schema)?;
        assert_eq!(field.name(), &expr.to_string());
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable());
        assert_eq!(extension_name(&field), Some("geoarrow.wkb"));

        // A NULL branch does not erase the other branch's extension type
        let field = IfExpr::new(c("p"), c("g1"), lit(ScalarValue::Null)).return_field(&schema)?;
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(extension_name(&field), Some("geoarrow.wkb"));

        // Differing extension types are a planning error
        let err = IfExpr::new(c("p"), c("g1"), c("j"))
            .return_field(&schema)
            .unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{err}");
        assert!(err.to_string().contains("geoarrow.wkb"), "{err}");
        assert!(err.to_string().contains("arrow.json"), "{err}");
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
