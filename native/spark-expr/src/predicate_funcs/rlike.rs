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
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, StringArrayType};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion::common::{internal_err, Result, ScalarValue};
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
    fn is_match<'a, S>(&self, inputs: &'a S) -> BooleanArray
    where
        &'a S: StringArrayType<'a>,
    {
        inputs
            .iter()
            .map(|v| v.map(|s| self.pattern.is_match(s)))
            .collect()
    }

    fn is_match_array(&self, array: &ArrayRef) -> Result<BooleanArray> {
        match array.data_type() {
            DataType::Utf8 => Ok(self.is_match(as_string_array(array)?)),
            DataType::LargeUtf8 => Ok(self.is_match(as_large_string_array(array)?)),
            DataType::Utf8View => Ok(self.is_match(as_string_view_array(array)?)),
            other => {
                internal_err!("RLike requires string type for input, got {other:?}")
            }
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
                let dict_array = array.as_any_dictionary();
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
    use arrow::array::{
        DictionaryArray, Int32Array, Int8Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::{Field, Int32Type, Int8Type};
    use datafusion::physical_expr::expressions::{Column, Literal};

    fn assert_bool_results(result: ColumnarValue, expected: &[Option<bool>]) {
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array result");
        };
        let bools = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");
        assert_eq!(bools.len(), expected.len());
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!bools.is_null(i), "row {i} should not be null");
                    assert_eq!(bools.value(i), *v, "row {i}");
                }
                None => assert!(bools.is_null(i), "row {i} should be null"),
            }
        }
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
    fn test_rlike_string_array_layouts() {
        let pattern = "R[a-z]+";
        let cases: Vec<(DataType, ArrayRef)> = vec![
            (
                DataType::Utf8,
                Arc::new(StringArray::from(vec![Some("Rose"), None, Some("Daisy")])),
            ),
            (
                DataType::LargeUtf8,
                Arc::new(LargeStringArray::from(vec![
                    Some("Rose"),
                    None,
                    Some("Daisy"),
                ])),
            ),
            (
                DataType::Utf8View,
                Arc::new(StringViewArray::from(vec![
                    Some("Rose"),
                    None,
                    Some("Daisy"),
                ])),
            ),
        ];

        for (data_type, array) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new("s", data_type, true)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let expr = RLike::try_new(Arc::new(Column::new("s", 0)), pattern).unwrap();
            assert_bool_results(
                expr.evaluate(&batch).unwrap(),
                &[Some(true), None, Some(false)],
            );
        }
    }

    #[test]
    fn test_rlike_string_array_no_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["Rose", "Daisy"]))],
        )
        .unwrap();

        let expr = RLike::try_new(Arc::new(Column::new("s", 0)), "R[a-z]+").unwrap();
        let ColumnarValue::Array(arr) = expr.evaluate(&batch).unwrap() else {
            panic!("expected array result");
        };
        // All-valid input must not allocate a null buffer (filter fast path).
        assert!(arr.nulls().is_none());
        assert_bool_results(ColumnarValue::Array(arr), &[Some(true), Some(false)]);
    }

    #[test]
    fn test_rlike_dictionary_arrays() {
        let pattern = "R[a-z]+";
        let expected = [Some(true), None, Some(false)];

        let utf8_values: ArrayRef = Arc::new(StringArray::from(vec!["Rose", "Daisy"]));
        let utf8_view_values: ArrayRef = Arc::new(StringViewArray::from(vec!["Rose", "Daisy"]));
        // Null in dictionary values (keys all valid): is_match emits null, take carries it.
        let utf8_values_with_null: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Rose"), None, Some("Daisy")]));

        let cases: Vec<(DataType, ArrayRef)> = vec![
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![Some(0), None, Some(1)]),
                    Arc::clone(&utf8_values),
                )),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View)),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![Some(0), None, Some(1)]),
                    Arc::clone(&utf8_view_values),
                )),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                Arc::new(DictionaryArray::<Int8Type>::new(
                    Int8Array::from(vec![Some(0), None, Some(1)]),
                    Arc::clone(&utf8_values),
                )),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![Some(0), Some(1), Some(2)]),
                    utf8_values_with_null,
                )),
            ),
        ];

        for (data_type, array) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new("s", data_type, true)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let expr = RLike::try_new(Arc::new(Column::new("s", 0)), pattern).unwrap();
            assert_bool_results(expr.evaluate(&batch).unwrap(), &expected);
        }
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
