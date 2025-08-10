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

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::any::Any;
use std::sync::Arc;

use crate::bloom_filter::spark_bloom_filter::SparkBloomFilter;

#[derive(Debug)]
pub struct BloomFilterMightContain {
    signature: Signature,
    bloom_filter: Option<SparkBloomFilter>,
}

impl BloomFilterMightContain {
    pub fn try_new(bloom_filter_expr: Arc<dyn PhysicalExpr>) -> Result<Self> {
        // early evaluate the bloom_filter_expr to get the actual bloom filter
        let bloom_filter = evaluate_bloom_filter(&bloom_filter_expr)?;
        Ok(Self {
            bloom_filter,
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Int64],
                Volatility::Immutable,
            ),
        })
    }
}

fn evaluate_bloom_filter(
    bloom_filter_expr: &Arc<dyn PhysicalExpr>,
) -> Result<Option<SparkBloomFilter>> {
    // bloom_filter_expr must be a literal/scalar subquery expression, so we can evaluate it
    // with an empty batch with empty schema
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let bloom_filter_bytes = bloom_filter_expr.evaluate(&batch)?;
    match bloom_filter_bytes {
        ColumnarValue::Scalar(ScalarValue::Binary(v)) => {
            Ok(v.map(|v| SparkBloomFilter::from(v.as_slice())))
        }
        _ => internal_err!("Bloom filter expression should be evaluated as a scalar binary value"),
    }
}

impl ScalarUDFImpl for BloomFilterMightContain {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "might_contain"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        execute_might_contain(&self.bloom_filter, &args.args)
    }
}

fn execute_might_contain(
    bloom_filter: &Option<SparkBloomFilter>,
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Int64(optional_value)) => {
            let result = bloom_filter
                .as_ref()
                .and_then(|filter| optional_value.map(|value| filter.might_contain_long(value)));
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
        }
        ColumnarValue::Array(values_array) => {
            let values = values_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Expected values Array to be an Int64Array".to_lowercase(),
                    )
                })?;

            bloom_filter
                .as_ref()
                .map(|filter| {
                    Ok(ColumnarValue::Array(Arc::new(
                        filter.might_contain_longs(values),
                    )))
                })
                .unwrap_or_else(|| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))))
        }
        _ => internal_err!("Expected Int64Array or Int64 Scalar as arguments"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;

    fn assert_result_eq<T: Into<Option<bool>>>(result: ColumnarValue, expected: Vec<T>) {
        let array = result.to_array(1).unwrap();
        let booleans = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let expected_optionals: Vec<Option<bool>> =
            expected.into_iter().map(|b| b.into()).collect();
        assert_eq!(booleans, &BooleanArray::from(expected_optionals));
    }

    fn assert_all_null(result: ColumnarValue) {
        let array = result.to_array(1).unwrap();
        let booleans = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(booleans.null_count(), booleans.len());
    }

    #[test]
    fn test_execute_scalar_contained() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);
        filter.put_long(456);
        filter.put_long(789);

        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(123)))];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_result_eq(result, vec![true]);
    }

    #[test]
    fn test_execute_scalar_not_contained() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);
        filter.put_long(456);
        filter.put_long(789);

        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(999)))];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_result_eq(result, vec![false]);
    }

    #[test]
    fn test_execute_scalar_null() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);
        filter.put_long(456);
        filter.put_long(789);

        let args = [ColumnarValue::Scalar(ScalarValue::Int64(None))];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_all_null(result);
    }

    #[test]
    fn test_execute_array() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);
        filter.put_long(456);
        filter.put_long(789);

        let values = Int64Array::from(vec![123, 999, 789]);

        let args = [ColumnarValue::Array(Arc::new(values))];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_result_eq(result, vec![true, false, true]);
    }

    #[test]
    fn test_execute_array_partially_null() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);
        filter.put_long(456);
        filter.put_long(789);

        let values = Int64Array::from(vec![Some(123), None, Some(555)]);

        let args = [ColumnarValue::Array(Arc::new(values))];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_result_eq(result, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn test_execute_scalar_missing_filter() {
        let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(123)))];

        let result = execute_might_contain(&None, &args).unwrap();
        assert_all_null(result);
    }

    #[test]
    fn test_execute_array_missing_filter() {
        let values = Int64Array::from(vec![123, 999, 789]);

        let args = [ColumnarValue::Array(Arc::new(values))];

        let result = execute_might_contain(&None, &args).unwrap();
        assert_all_null(result);
    }
}
