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
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::DataType;
use arrow::{
    array::{AsArray, Decimal128Array},
    datatypes::{validate_decimal_precision, Decimal128Type, DecimalType, Int64Type},
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

                // The Int64 is already the unscaled Decimal128 value; this widens the bits
                // (an Arrow Int64->Decimal cast would rescale). Infallible so it vectorizes.
                let widened: Decimal128Array = unary::<_, _, Decimal128Type>(arr, |v| v as i128);

                // `.iter().flatten()` skips null slots so garbage under a null cannot
                // trigger a false overflow. `find` short-circuits like `.all(is_valid)`
                // while also handing back the value needed for the ANSI error message.
                let first_offender = widened
                    .iter()
                    .flatten()
                    .find(|v| !Decimal128Type::is_valid_decimal_precision(*v, precision));

                let result = match (first_offender, fail_on_error) {
                    // No overflow: attach metadata. `with_precision_and_scale` would rescan.
                    (None, _) => widened.with_data_type(result_type),
                    (Some(v), true) => {
                        return Err(DataFusionError::External(Box::new(decimal_overflow_error(
                            v, precision, scale,
                        ))));
                    }
                    (Some(_), false) => widened
                        .null_if_overflow_precision(precision)
                        .with_data_type(result_type),
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
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
    use arrow::array::{Array, Decimal128Array, Int64Array};

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

    #[test]
    fn test_scalar_valid_value() {
        // Locks the happy path for the scalar branch: an in-range value round-trips
        // to the correct Decimal128 with matching precision/scale. Existing scalar
        // tests only cover overflow and null input.
        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(999)))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), true)
            .expect("in-range scalar should succeed");
        let ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) = result else {
            panic!("expected decimal scalar result")
        };
        assert_eq!(v, Some(999));
    }

    #[test]
    fn test_scalar_overflow_nulls_when_not_fail_on_error() {
        // Covers `long_to_decimal`'s `Err(_) => Ok(None)` branch through the scalar
        // path. The array path exercises this branch indirectly via `null_if_overflow_precision`;
        // the scalar branch has its own code and was previously untested.
        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(123456)))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("scalar overflow without fail_on_error should return null");
        let ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) = result else {
            panic!("expected decimal scalar result")
        };
        assert_eq!(v, None);
    }

    #[test]
    fn test_array_no_overflow_fast_path() {
        // Common path: no overflow, no nulls. Values pass through unchanged and the
        // output carries the target Decimal128 type. Locks the (None, _) arm that
        // existing overflow tests never touch.
        let args = [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(1i64),
            Some(50),
            Some(999),
        ])))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("no overflow should succeed");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert_eq!(array.data_type(), &DataType::Decimal128(3, 0));
        assert_eq!(array.null_count(), 0);
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.value(0), 1);
        assert_eq!(decimals.value(1), 50);
        assert_eq!(decimals.value(2), 999);
    }

    #[test]
    fn test_array_negative_overflow_nulls() {
        // `is_valid_decimal_precision` checks both bounds; -1000 is below the
        // Decimal128(3, 0) lower bound (-999). Guards the negative branch that
        // existing overflow tests (all positive) do not cover.
        let args = [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(-1000i64),
            Some(5),
        ])))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("negative overflow should become null");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert!(array.is_null(0), "negative overflow should be nulled");
        assert!(array.is_valid(1));
    }

    #[test]
    fn test_array_null_slot_garbage_not_scanned() {
        // Regression guard for the `.iter().flatten()` decision. `unary` widens
        // every slot including nulls, so the null slot's underlying storage can
        // hold arbitrary bits. If the scan ever switched to `.values().iter()`,
        // i64::MAX under the null would falsely trip Decimal128(3, 0) overflow.
        use arrow::buffer::{NullBuffer, ScalarBuffer};

        let values = ScalarBuffer::from(vec![10i64, i64::MAX, 20i64]);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let arr = Int64Array::new(values, Some(nulls));
        let args = [ColumnarValue::Array(Arc::new(arr))];

        // Non-ANSI: no overflow detected, null slot stays null (not double-nullified).
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("null slot garbage must not trigger overflow");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert!(array.is_valid(0));
        assert!(array.is_null(1));
        assert!(array.is_valid(2));

        // ANSI: also must not raise on garbage hidden behind a null.
        spark_make_decimal(&args, &DataType::Decimal128(3, 0), true)
            .expect("ANSI mode must not error on null slot garbage");
    }

    #[test]
    fn test_array_boundary_precision() {
        // 999 is exactly the max for Decimal128(3, 0); 1000 is over by one.
        // Pins the off-by-one on `is_valid_decimal_precision`.
        let args = [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(999i64),
            Some(1000),
        ])))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("boundary case should not error in non-ANSI");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert!(array.is_valid(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn test_array_all_null() {
        // .iter().flatten() on an all-null array yields nothing, so `find` returns
        // None and the fast path is taken. Locks that the all-null mask and target
        // type are preserved.
        let args = [ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            None::<i64>,
            None,
            None,
        ])))];
        let result = spark_make_decimal(&args, &DataType::Decimal128(3, 0), false)
            .expect("all-null should succeed");
        let ColumnarValue::Array(array) = result else {
            panic!("expected array result")
        };
        assert_eq!(array.data_type(), &DataType::Decimal128(3, 0));
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 3);
    }
}
