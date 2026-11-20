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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_common::error_chain;
use datafusion_comet_jni_bridge::errors::CometError;

use crate::{SparkError, SparkErrorWithContext};

/// Spark's `CheckOverflowInTableInsert`: evaluates the cast that a table insert added and turns
/// any `SparkArithmeticException` from it into `CAST_OVERFLOW_IN_TABLE_INSERT`, which names the
/// column instead of the value and carries no query context.
#[derive(Debug)]
pub struct CheckOverflowInTableInsert {
    child: Arc<dyn PhysicalExpr>,
    from_type: String,
    to_type: String,
    column_name: String,
}

impl CheckOverflowInTableInsert {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        from_type: String,
        to_type: String,
        column_name: String,
    ) -> Self {
        Self {
            child,
            from_type,
            to_type,
            column_name,
        }
    }

    fn table_insert_error(&self) -> DataFusionError {
        SparkError::CastOverflowInTableInsert {
            from_type: self.from_type.clone(),
            to_type: self.to_type.clone(),
            column_name: self.column_name.clone(),
        }
        .into()
    }
}

impl PartialEq for CheckOverflowInTableInsert {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.from_type == other.from_type
            && self.to_type == other.to_type
            && self.column_name == other.column_name
    }
}

impl Eq for CheckOverflowInTableInsert {}

impl Hash for CheckOverflowInTableInsert {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.from_type.hash(state);
        self.to_type.hash(state);
        self.column_name.hash(state);
    }
}

impl Display for CheckOverflowInTableInsert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckOverflowInTableInsert({}, column: {})",
            self.child, self.column_name
        )
    }
}

impl PhysicalExpr for CheckOverflowInTableInsert {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.child.fmt_sql(f)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.child.evaluate(batch) {
            Err(e) if is_arithmetic_error(&e) => Err(self.table_insert_error()),
            other => other,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match children.as_slice() {
            [child] => Ok(Arc::new(Self::new(
                Arc::clone(child),
                self.from_type.clone(),
                self.to_type.clone(),
                self.column_name.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "CheckOverflowInTableInsert should have exactly one child".to_string(),
            )),
        }
    }
}

/// Whether Spark raises this error as a `SparkArithmeticException`: a native `SparkError`, or an
/// exception thrown by a child that runs Spark's own code through the JVM codegen dispatcher.
/// Like the JNI layer, it takes the first Spark error in the `source()` chain.
fn is_arithmetic_error(error: &DataFusionError) -> bool {
    error_chain(error)
        .find_map(|e| {
            if let Some(CometError::JavaException { class, .. }) = e.downcast_ref::<CometError>() {
                Some(SparkError::is_arithmetic_exception_class(
                    &class.replace('.', "/"),
                ))
            } else if let Some(e) = e.downcast_ref::<SparkErrorWithContext>() {
                Some(e.error.is_arithmetic())
            } else {
                e.downcast_ref::<SparkError>()
                    .map(SparkError::is_arithmetic)
            }
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryContext;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Failure {
        CastOverflow,
        DecimalOutOfRange,
        DivideByZero,
        InvalidInput,
    }

    impl Failure {
        fn error(self) -> SparkError {
            match self {
                Failure::CastOverflow => SparkError::CastOverFlow {
                    value: "3000000000L".to_string(),
                    from_type: "BIGINT".to_string(),
                    to_type: "INT".to_string(),
                },
                Failure::DecimalOutOfRange => SparkError::NumericValueOutOfRange {
                    value: "1234567.89".to_string(),
                    precision: 10,
                    scale: 2,
                },
                Failure::DivideByZero => SparkError::DivideByZero,
                Failure::InvalidInput => SparkError::CastInvalidValue {
                    value: "'abc'".to_string(),
                    from_type: "STRING".to_string(),
                    to_type: "INT".to_string(),
                },
            }
        }
    }

    /// A child that returns column `c`, or fails. A wrapped error comes inside
    /// `DataFusionError::Context`, as DataFusion adds it on the way up.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Child {
        failure: Option<Failure>,
        with_context: bool,
        wrapped: bool,
    }

    impl Display for Child {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "child")
        }
    }

    impl PhysicalExpr for Child {
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "child")
        }

        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            Ok(true)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            let Some(failure) = self.failure else {
                return Column::new("c", 0).evaluate(batch);
            };
            let error = failure.error();
            let error = if self.with_context {
                let context = QueryContext::new("SELECT c".to_string(), 7, 7, None, None, 1, 7);
                SparkErrorWithContext::with_context(error, Arc::new(context)).into()
            } else {
                error.into()
            };
            if self.wrapped {
                Err(DataFusionError::Context(
                    "child".to_string(),
                    Box::new(error),
                ))
            } else {
                Err(error)
            }
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
    }

    fn evaluate(
        failure: Option<Failure>,
        with_context: bool,
        wrapped: bool,
    ) -> Result<ColumnarValue> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]))],
        )
        .unwrap();
        let child = Child {
            failure,
            with_context,
            wrapped,
        };
        CheckOverflowInTableInsert::new(
            Arc::new(child),
            "BIGINT".to_string(),
            "INT".to_string(),
            "c".to_string(),
        )
        .evaluate(&batch)
    }

    /// The first Spark error's message, and whether it carries a query context.
    fn spark_error(result: Result<ColumnarValue>) -> (String, bool) {
        let Err(error) = result else {
            panic!("expected a SparkError, got {result:?}");
        };
        let found = error_chain(&error)
            .find_map(|e| {
                if let Some(e) = e.downcast_ref::<SparkErrorWithContext>() {
                    Some((e.error.to_string(), e.context.is_some()))
                } else {
                    e.downcast_ref::<SparkError>()
                        .map(|e| (e.to_string(), false))
                }
            })
            .expect("a SparkError");
        found
    }

    #[test]
    fn values_pass_through() {
        let ColumnarValue::Array(array) = evaluate(None, false, false).unwrap() else {
            panic!("expected an array");
        };
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(array, &Int64Array::from(vec![Some(1), None, Some(-2)]));
    }

    #[test]
    fn arithmetic_errors_become_table_insert_errors() {
        let failures = [
            Failure::CastOverflow,
            Failure::DecimalOutOfRange,
            Failure::DivideByZero,
        ];
        for failure in failures {
            for (with_context, wrapped) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let clue = format!("{failure:?} with context {with_context}, wrapped {wrapped}");
                let result = evaluate(Some(failure), with_context, wrapped);
                let (message, has_context) = spark_error(result);
                assert_eq!(
                    message,
                    "[CAST_OVERFLOW_IN_TABLE_INSERT] Fail to assign a value of \"BIGINT\" type \
                     to the \"INT\" type column or variable `c` due to an overflow. Use \
                     `try_cast` on the input value to tolerate overflow and return NULL instead.",
                    "{clue}"
                );
                assert!(!has_context, "{clue}");
            }
        }
    }

    #[test]
    fn other_errors_are_unchanged() {
        for (with_context, wrapped) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let result = evaluate(Some(Failure::InvalidInput), with_context, wrapped);
            assert_eq!(matches!(result, Err(DataFusionError::Context(..))), wrapped);
            let (message, has_context) = spark_error(result);
            assert_eq!(message, Failure::InvalidInput.error().to_string());
            assert_eq!(has_context, with_context);
        }
    }
}
