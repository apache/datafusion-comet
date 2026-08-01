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

use crate::error::decimal_overflow_error;
use crate::math_funcs::utils::get_precision_scale;
use arrow::compute::kernels::arity::try_unary;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::{
    array::{AsArray, Decimal128Array},
    datatypes::{validate_decimal_precision, Decimal128Type, Int64Type},
};
use datafusion::common::{internal_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark-compatible `MakeDecimal` expression (internal to Spark optimizer)
pub fn spark_make_decimal(
    args: &[ColumnarValue],
    data_type: &DataType,
    fail_on_error: bool,
) -> DataFusionResult<ColumnarValue> {
    let (precision, scale) = get_precision_scale(data_type);
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Int64(n) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                long_to_decimal(*n, precision, scale, fail_on_error)?,
                precision,
                scale,
            ))),
            sv => internal_err!("Expected Int64 but found {sv:?}"),
        },
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Int64 => {
                let arr = a.as_primitive::<Int64Type>();
                let result_type = DataType::Decimal128(precision, scale);
                // The Int64 is already the unscaled Decimal128 value, so we only reinterpret
                // the bits (an Arrow Int64->Decimal cast would rescale the value). Both arity
                // helpers reuse the input null buffer and only invoke the closure on valid rows.
                let result: Decimal128Array = if fail_on_error {
                    // ANSI mode: overflow is a hard error. `try_unary` surfaces the closure's
                    // ArrowError; unwrap the ExternalError back to DataFusionError::External so
                    // the ANSI error variant (not a generic ArrowError) is preserved.
                    try_unary::<Int64Type, _, Decimal128Type>(arr, |v| {
                        let v = v as i128;
                        validate_decimal_precision(v, precision, scale)
                            .map(|()| v)
                            .map_err(|_| {
                                ArrowError::ExternalError(Box::new(decimal_overflow_error(
                                    v, precision, scale,
                                )))
                            })
                    })
                    .map_err(|e| match e {
                        ArrowError::ExternalError(inner) => DataFusionError::External(inner),
                        other => DataFusionError::from(other),
                    })?
                } else {
                    // Non-ANSI: overflow becomes null. `unary_opt` applies the closure only to
                    // valid rows and marks a row null wherever the closure returns None.
                    arr.unary_opt::<_, Decimal128Type>(|v| {
                        let v = v as i128;
                        validate_decimal_precision(v, precision, scale)
                            .ok()
                            .map(|()| v)
                    })
                };

                Ok(ColumnarValue::Array(Arc::new(
                    result.with_data_type(result_type),
                )))
            }
            av => internal_err!("Expected Int64 but found {av:?}"),
        },
    }
}

/// Convert the input long to decimal with the given maximum precision. On overflow, errors when
/// `fail_on_error` is set (Spark's `nullOnOverflow = false`, i.e. ANSI mode) and returns null
/// otherwise.
#[inline]
fn long_to_decimal(
    v: Option<i64>,
    precision: u8,
    scale: i8,
    fail_on_error: bool,
) -> DataFusionResult<Option<i128>> {
    v.map_or(Ok(None), |v| {
        let v = v as i128;
        match validate_decimal_precision(v, precision, scale) {
            Ok(()) => Ok(Some(v)),
            Err(_) if fail_on_error => Err(DataFusionError::External(Box::new(
                decimal_overflow_error(v, precision, scale),
            ))),
            Err(_) => Ok(None),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};

    fn overflow_args() -> [ColumnarValue; 1] {
        // 123456 does not fit Decimal128(3, 0)
        [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(123456),
            None,
            Some(99),
        ])))]
    }

    #[test]
    fn test_array_overflow_errors_when_fail_on_error() {
        let err = spark_make_decimal(&overflow_args(), &DataType::Decimal128(3, 0), true)
            .expect_err("overflow should error when fail_on_error is set");
        assert!(
            err.to_string().contains("NUMERIC_VALUE_OUT_OF_RANGE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_array_overflow_reports_first_offending_value() {
        // Two distinct overflowing values with a valid value in front. Both the original
        // per-row `?` loop and `try_unary` walk rows in index order, so the reported error
        // must reference the FIRST overflow (111111), not the later one (222222). This locks
        // the ordering semantics rather than merely "some error occurred".
        let args = [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(99),
            Some(111111),
            Some(222222),
        ])))];
        let err = spark_make_decimal(&args, &DataType::Decimal128(3, 0), true)
            .expect_err("overflow should error when fail_on_error is set");
        let msg = err.to_string();
        assert!(
            msg.contains("111111"),
            "should report first overflow: {msg}"
        );
        assert!(
            !msg.contains("222222"),
            "should not report later overflow: {msg}"
        );
    }

    #[test]
    fn test_array_overflow_nulls_when_not_fail_on_error() {
        let result = spark_make_decimal(&overflow_args(), &DataType::Decimal128(3, 0), false)
            .expect("overflow should become null without fail_on_error");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert!(array.is_valid(2));
    }

    #[test]
    fn test_scalar_overflow_errors_when_fail_on_error() {
        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(123456)))];
        let err = spark_make_decimal(&args, &DataType::Decimal128(3, 0), true)
            .expect_err("overflow should error when fail_on_error is set");
        assert!(
            err.to_string().contains("NUMERIC_VALUE_OUT_OF_RANGE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_scalar_null_is_not_an_error() {
        let args = [ColumnarValue::Scalar(ScalarValue::Int64(None))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), true)
            .expect("null input should not error");
        let ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) = result else {
            panic!("expected decimal scalar result")
        };
        assert!(v.is_none());
    }
}
