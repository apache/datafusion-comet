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

use arrow::datatypes::{DataType, Schema};
use arrow::{
    array::{as_primitive_array, Array, ArrayRef, Decimal128Array},
    datatypes::{Decimal128Type, DecimalType},
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;

use crate::SparkError;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

/// This is from Spark `CheckOverflow` expression. Spark `CheckOverflow` expression rounds decimals
/// to given scale and check if the decimals can fit in given precision. As `cast` kernel rounds
/// decimals already, Comet `CheckOverflow` expression only checks if the decimals can fit in the
/// precision.
#[derive(Debug, Eq)]
pub struct CheckOverflow {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub fail_on_error: bool,
    pub expr_id: Option<u64>,
    pub query_context: Option<Arc<crate::QueryContext>>,
}

impl Hash for CheckOverflow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for CheckOverflow {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl CheckOverflow {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        fail_on_error: bool,
        expr_id: Option<u64>,
        query_context: Option<Arc<crate::QueryContext>>,
    ) -> Self {
        Self {
            child,
            data_type,
            fail_on_error,
            expr_id,
            query_context,
        }
    }
}

impl Display for CheckOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckOverflow [datatype: {}, fail_on_error: {}, child: {}]",
            self.data_type, self.fail_on_error, self.child
        )
    }
}

impl PhysicalExpr for CheckOverflow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
            {
                let (precision, scale) = match &self.data_type {
                    DataType::Decimal128(p, s) => (p, s),
                    dt => {
                        return Err(DataFusionError::Execution(format!(
                            "CheckOverflow expects only Decimal128, but got {dt:?}"
                        )))
                    }
                };

                let decimal_array = as_primitive_array::<Decimal128Type>(&array);

                let casted_array = if self.fail_on_error {
                    // Returning error if overflow - convert decimal overflow to SparkError
                    decimal_array
                        .validate_decimal_precision(*precision)
                        .map_err(|e| {
                            if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                                && e.to_string().contains("too large to store in a Decimal128") {
                                // Find the first overflowing value
                                let overflow_value = decimal_array
                                    .iter()
                                    .find(|v| {
                                        if let Some(val) = v {
                                            arrow::array::types::Decimal128Type::validate_decimal_precision(
                                                *val, *precision, *scale
                                            ).is_err()
                                        } else {
                                            false
                                        }
                                    })
                                    .and_then(|v| v)
                                    .unwrap_or(0);

                                let spark_error = crate::error::decimal_overflow_error(overflow_value, *precision, *scale);

                                // Wrap with query_context if present
                                if let Some(ctx) = &self.query_context {
                                    DataFusionError::External(Box::new(
                                        crate::SparkErrorWithContext::with_context(spark_error, Arc::clone(ctx))
                                    ))
                                } else {
                                    DataFusionError::External(Box::new(spark_error))
                                }
                            } else {
                                DataFusionError::ArrowError(Box::new(e), None)
                            }
                        })?;
                    decimal_array
                } else {
                    // Overflowing gets null value
                    &decimal_array.null_if_overflow_precision(*precision)
                };

                let new_array = Decimal128Array::from(casted_array.into_data())
                    .with_precision_and_scale(*precision, *scale)
                    .map(|a| Arc::new(a) as ArrayRef)
                    .map_err(|e| {
                        if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                            && e.to_string().contains("too large to store in a Decimal128")
                        {
                            // Fallback error handling
                            let spark_error = SparkError::NumericValueOutOfRange {
                                value: "overflow".to_string(),
                                precision: *precision,
                                scale: *scale,
                            };

                            // Wrap with query_context if present
                            if let Some(ctx) = &self.query_context {
                                DataFusionError::External(Box::new(
                                    crate::SparkErrorWithContext::with_context(
                                        spark_error,
                                        Arc::clone(ctx),
                                    ),
                                ))
                            } else {
                                DataFusionError::External(Box::new(spark_error))
                            }
                        } else {
                            DataFusionError::ArrowError(Box::new(e), None)
                        }
                    })?;

                Ok(ColumnarValue::Array(new_array))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, precision, scale)) => {
                if self.fail_on_error {
                    if let Some(val) = v {
                        Decimal128Type::validate_decimal_precision(val, precision, scale).map_err(
                            |_| {
                                let spark_error =
                                    crate::error::decimal_overflow_error(val, precision, scale);
                                if let Some(ctx) = &self.query_context {
                                    DataFusionError::External(Box::new(
                                        crate::SparkErrorWithContext::with_context(
                                            spark_error,
                                            Arc::clone(ctx),
                                        ),
                                    ))
                                } else {
                                    DataFusionError::External(Box::new(spark_error))
                                }
                            },
                        )?;
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        v, precision, scale,
                    )))
                } else {
                    let new_v: Option<i128> = v.and_then(|v| {
                        Decimal128Type::validate_decimal_precision(v, precision, scale)
                            .map(|_| v)
                            .ok()
                    });
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        new_v, precision, scale,
                    )))
                }
            }
            v => Err(DataFusionError::Execution(format!(
                "CheckOverflow's child expression should be decimal array, but found {v:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CheckOverflow::new(
            Arc::clone(&children[0]),
            self.data_type.clone(),
            self.fail_on_error,
            self.expr_id,
            self.query_context.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::fmt::{Display, Formatter};

    /// Helper that always returns a fixed Decimal128 scalar.
    #[derive(Debug, Eq, PartialEq, Hash)]
    struct ScalarChild(Option<i128>, u8, i8);

    impl Display for ScalarChild {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ScalarChild({:?})", self.0)
        }
    }

    impl PhysicalExpr for ScalarChild {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
            Ok(DataType::Decimal128(self.1, self.2))
        }
        fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
            Ok(true)
        }
        fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                self.0, self.1, self.2,
            )))
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    fn empty_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("x", DataType::Decimal128(38, 0), true)]);
        RecordBatch::new_empty(Arc::new(schema))
    }

    fn make_check_overflow(
        value: Option<i128>,
        precision: u8,
        scale: i8,
        fail_on_error: bool,
    ) -> CheckOverflow {
        CheckOverflow::new(
            Arc::new(ScalarChild(value, precision, scale)),
            DataType::Decimal128(precision, scale),
            fail_on_error,
            None,
            None,
        )
    }

    // --- scalar, fail_on_error = false (legacy mode) ---

    #[test]
    fn test_scalar_no_overflow_legacy() {
        // 999 fits in precision 3, scale 0 → returned as-is
        let expr = make_check_overflow(Some(999), 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, Some(999)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_overflow_returns_null_in_legacy_mode() {
        // 1000 does not fit in precision 3 → null, no error
        let expr = make_check_overflow(Some(1000), 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_null_passthrough_legacy() {
        let expr = make_check_overflow(None, 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // --- scalar, fail_on_error = true (ANSI mode) ---

    #[test]
    fn test_scalar_no_overflow_ansi() {
        // 999 fits in precision 3 → returned as-is, no error
        let expr = make_check_overflow(Some(999), 3, 0, true);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, Some(999)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_overflow_returns_error_in_ansi_mode() {
        // 1000 does not fit in precision 3 → error, not Ok(None)
        // This is the case that previously panicked with "fail_on_error (ANSI mode) is not
        // supported yet".
        let expr = make_check_overflow(Some(1000), 3, 0, true);
        let result = expr.evaluate(&empty_batch());
        assert!(result.is_err(), "expected error on overflow in ANSI mode");
    }

    #[test]
    fn test_scalar_null_passthrough_ansi() {
        // None input → None output even in ANSI mode (no value to overflow)
        let expr = make_check_overflow(None, 3, 0, true);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
