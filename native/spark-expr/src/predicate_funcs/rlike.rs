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

use crate::SparkError;
use arrow::array::builder::BooleanBuilder;
use arrow::array::types::Int32Type;
use arrow::array::{
    Array, ArrayAccessor, ArrayRef, BooleanArray, RecordBatch, StringArrayType,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::cast::{
    as_dictionary_array, as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion::common::{exec_err, internal_err, Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use regex::Regex;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Implementation of RLIKE operator.
///
/// Note that this implementation is not yet Spark-compatible and simply delegates to
/// the Rust regexp crate. It will match Spark behavior for some simple cases but has
/// differences in whitespace handling and does not support all the features of Java's
/// regular expression engine, which are documented at:
///
/// https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
///
/// Array matching keeps the plan-time compiled [`Regex`] and loops over Utf8 /
/// LargeUtf8 / Utf8View inputs. Arrow's `regexp_is_match(_scalar)` was considered
/// but recompiles the pattern per batch; criterion benches showed regressions on
/// common patterns such as character classes (see issue #5102).
#[derive(Debug)]
pub struct RLike {
    child: Arc<dyn PhysicalExpr>,
    // Only scalar patterns are supported
    pattern_str: String,
    pattern: Regex,
}

impl PartialEq for RLike {
    fn eq(&self, other: &Self) -> bool {
        *(self.child) == *(other.child) && self.pattern_str == other.pattern_str
    }
}

impl Eq for RLike {}

impl Hash for RLike {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.pattern_str.hash(state);
    }
}

impl RLike {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, pattern: &str) -> Result<Self> {
        Ok(Self {
            child,
            pattern_str: pattern.to_string(),
            pattern: Regex::new(pattern).map_err(|e| {
                SparkError::Internal(format!("Failed to compile pattern {pattern}: {e}"))
            })?,
        })
    }

    /// Match the pre-compiled pattern against a string array of any Arrow string layout.
    ///
    /// Keeps the plan-time compiled [`Regex`] rather than calling Arrow's
    /// `regexp_is_match(_scalar)`, which recompiles the pattern on every batch.
    fn is_match<'a, S>(&'a self, inputs: &'a S) -> BooleanArray
    where
        &'a S: StringArrayType<'a>,
    {
        let mut builder = BooleanBuilder::with_capacity(inputs.len());
        if inputs.is_nullable() {
            for i in 0..inputs.len() {
                if inputs.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(self.pattern.is_match(inputs.value(i)));
                }
            }
        } else {
            for i in 0..inputs.len() {
                builder.append_value(self.pattern.is_match(inputs.value(i)));
            }
        }
        builder.finish()
    }

    fn is_match_array(&self, array: &ArrayRef) -> Result<BooleanArray> {
        match array.data_type() {
            DataType::Utf8 => Ok(self.is_match(as_string_array(array)?)),
            DataType::LargeUtf8 => Ok(self.is_match(as_large_string_array(array)?)),
            DataType::Utf8View => Ok(self.is_match(as_string_view_array(array)?)),
            other => exec_err!("RLike requires string type for input, got {other:?}"),
        }
    }
}

impl Display for RLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RLike [child: {}, pattern: {}] ",
            self.child, self.pattern_str
        )
    }
}

impl PhysicalExpr for RLike {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.child.evaluate(batch)? {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Dictionary(_, _)) =>
            {
                let dict_array = as_dictionary_array::<Int32Type>(&array)?;
                // evaluate the regexp pattern against the dictionary values
                let new_values = self.is_match_array(dict_array.values())?;
                // convert to conventional (not dictionary-encoded) array
                let result = take(&new_values, dict_array.keys(), None)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Array(array) => {
                let result = self.is_match_array(&array)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                let is_match = match scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => self.pattern.is_match(&s),
                    _ => {
                        return internal_err!(
                            "RLike requires string type for input, got {:?}",
                            scalar.data_type()
                        );
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(is_match))))
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
        assert!(children.len() == 1);
        Ok(Arc::new(RLike::try_new(
            Arc::clone(&children[0]),
            &self.pattern_str,
        )?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::{Column, Literal};

    fn assert_bool_array_rose_null(result: ColumnarValue) {
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array result");
        };
        let bools = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");
        assert_eq!(bools.len(), 2);
        assert!(bools.value(0));
        assert!(bools.is_null(1));
    }

    #[test]
    fn test_rlike_scalar_string_variants() {
        let pattern = "R[a-z]+";
        let scalars = [
            ScalarValue::Utf8(Some("Rose".to_string())),
            ScalarValue::LargeUtf8(Some("Rose".to_string())),
            ScalarValue::Utf8View(Some("Rose".to_string())),
        ];

        for scalar in scalars {
            let expr = RLike::try_new(Arc::new(Literal::new(scalar.clone())), pattern).unwrap();
            let result = expr
                .evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))
                .unwrap();
            let ColumnarValue::Scalar(result) = result else {
                panic!("expected scalar result");
            };
            assert_eq!(result, ScalarValue::Boolean(Some(true)));
        }

        // Null input should produce a null boolean result
        let expr =
            RLike::try_new(Arc::new(Literal::new(ScalarValue::Utf8(None))), pattern).unwrap();
        let result = expr
            .evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))
            .unwrap();
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected scalar result");
        };
        assert_eq!(result, ScalarValue::Boolean(None));
    }

    #[test]
    fn test_rlike_scalar_non_string_error() {
        let expr = RLike::try_new(
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            "R[a-z]+",
        )
        .unwrap();

        let result = expr.evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())));
        assert!(result.is_err());
    }

    #[test]
    fn test_rlike_utf8_array() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some("Rose"), None]))],
        )
        .unwrap();

        let expr = RLike::try_new(Arc::new(Column::new("s", 0)), "R[a-z]+").unwrap();
        assert_bool_array_rose_null(expr.evaluate(&batch).unwrap());
    }

    #[test]
    fn test_rlike_large_utf8_array() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeStringArray::from(vec![Some("Rose"), None]))],
        )
        .unwrap();

        let expr = RLike::try_new(Arc::new(Column::new("s", 0)), "R[a-z]+").unwrap();
        assert_bool_array_rose_null(expr.evaluate(&batch).unwrap());
    }

    #[test]
    fn test_rlike_utf8_view_array() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringViewArray::from(vec![Some("Rose"), None]))],
        )
        .unwrap();

        let expr = RLike::try_new(Arc::new(Column::new("s", 0)), "R[a-z]+").unwrap();
        assert_bool_array_rose_null(expr.evaluate(&batch).unwrap());
    }

    #[test]
    fn test_rlike_array_non_string_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
        )
        .unwrap();

        let expr = RLike::try_new(Arc::new(Column::new("b", 0)), "R[a-z]+").unwrap();
        assert!(expr.evaluate(&batch).is_err());
    }
}
