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

use arrow::array::AsArray;
use arrow::datatypes::{Decimal128Type, Int64Type};
use datafusion::common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark-compatible `UnscaledValue` expression (internal to Spark optimizer)
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Decimal128(d, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                d.map(|n| n as i64),
            ))),
            dt => internal_err!("Expected Decimal128 but found {dt:}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Decimal128Type>();
            Ok(ColumnarValue::Array(Arc::new(
                arr.unary::<_, Int64Type>(|v| v as i64),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Int64Array};

    #[test]
    fn test_unscaled_value_array_with_nulls() -> DataFusionResult<()> {
        let arr: Decimal128Array =
            vec![Some(12345), None, Some(-678), None, Some(i64::MAX as i128)]
                .into_iter()
                .collect::<Decimal128Array>()
                .with_precision_and_scale(20, 2)
                .unwrap();
        let args = vec![ColumnarValue::Array(Arc::new(arr))];
        let result = spark_unscaled_value(&args)?;

        let ColumnarValue::Array(result) = result else {
            panic!("Expected array result");
        };
        let result = result.as_primitive::<arrow::datatypes::Int64Type>();
        let expected = Int64Array::from(vec![Some(12345), None, Some(-678), None, Some(i64::MAX)]);
        assert_eq!(result, &expected);
        assert_eq!(result.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_unscaled_value_scalar() -> DataFusionResult<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(12345),
            20,
            2,
        ))];
        let result = spark_unscaled_value(&args)?;
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(12345)))
        ));

        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(None, 20, 2))];
        let result = spark_unscaled_value(&args)?;
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int64(None))
        ));
        Ok(())
    }
}
