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

use arrow::array::{Array, Int64Array, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

use crate::bloom_filter::spark_bloom_filter::SparkBloomFilter;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BloomFilterMightContain {
    signature: Signature,
    bloom_filter: Option<SparkBloomFilter>,
}

impl BloomFilterMightContain {
    pub fn is_null(&self) -> bool {
        self.bloom_filter.is_none()
    }

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
    // on one row with no input columns. Merged subquery fields return batch-sized arrays.
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let bloom_filter_bytes = match bloom_filter_expr.evaluate(&batch)? {
        ColumnarValue::Array(array) if array.len() == 1 => {
            ColumnarValue::Scalar(ScalarValue::try_from_array(array.as_ref(), 0)?)
        }
        value => value,
    };
    match bloom_filter_bytes {
        ColumnarValue::Scalar(ScalarValue::Binary(v)) => {
            Ok(v.map(|v| SparkBloomFilter::from(v.as_slice())))
        }
        // An untyped NULL literal, as in `might_contain(null, 1L)`
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        _ => internal_err!("Bloom filter expression should be evaluated as a scalar binary value"),
    }
}

impl ScalarUDFImpl for BloomFilterMightContain {
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
        // An untyped NULL literal, as in `might_contain(bf, null)`
        ColumnarValue::Scalar(ScalarValue::Null) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
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
        other => internal_err!("Expected Int64Array or Int64 Scalar as arguments, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom_filter::spark_bloom_filter::SparkBloomFilterVersion;
    use crate::{GetStructField, ScalarSubqueryConsumer};
    use arrow::array::{BinaryArray, BooleanArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Literal;

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
    fn test_execute_untyped_null_value() {
        let mut filter = SparkBloomFilter::from((4, 64));
        filter.put_long(123);

        let args = [ColumnarValue::Scalar(ScalarValue::Null)];

        let result = execute_might_contain(&Some(filter), &args).unwrap();
        assert_all_null(result);
    }

    #[test]
    fn test_untyped_null_bloom_filter() {
        let bloom_filter_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Null));
        let expr = BloomFilterMightContain::try_new(bloom_filter_expr).unwrap();
        assert!(expr.bloom_filter.is_none());
    }

    #[test]
    fn test_merged_scalar_bloom_filter() {
        for version in [SparkBloomFilterVersion::V1, SparkBloomFilterVersion::V2] {
            let mut filter = SparkBloomFilter::new(version, 4, 256, 0);
            filter.put_long(123);
            let bytes = filter.spark_serialization();
            for (value, parent_null) in [
                (Some(bytes.as_slice()), false),
                (None, false),
                (Some(bytes.as_slice()), true),
            ] {
                let structure = StructArray::new(
                    vec![Arc::new(Field::new("bloomFilter", DataType::Binary, true))].into(),
                    vec![Arc::new(BinaryArray::from(vec![value]))],
                    parent_null.then(|| NullBuffer::new_null(1)),
                );
                let field: Arc<dyn PhysicalExpr> = Arc::new(GetStructField::new(
                    Arc::new(Literal::new(ScalarValue::Struct(Arc::new(structure)))),
                    0,
                ));
                let field = ScalarSubqueryConsumer::wrap(field);
                let expr = BloomFilterMightContain::try_new(field).unwrap();
                let expected = if value.is_none() || parent_null {
                    None
                } else {
                    Some(true)
                };
                let args = [ColumnarValue::Scalar(ScalarValue::Int64(Some(123)))];
                assert_result_eq(
                    execute_might_contain(&expr.bloom_filter, &args).unwrap(),
                    vec![expected],
                );
                // Direct binary scalar subqueries retain the same result.
                let direct = Arc::new(Literal::new(ScalarValue::Binary(if parent_null {
                    None
                } else {
                    value.map(<[u8]>::to_vec)
                })));
                let direct = BloomFilterMightContain::try_new(direct).unwrap();
                assert_eq!(expr.bloom_filter, direct.bloom_filter);
            }
        }
    }

    #[test]
    fn test_non_binary_bloom_filter() {
        let value = Arc::new(Literal::new(ScalarValue::Int64(Some(123))));
        assert!(BloomFilterMightContain::try_new(value)
            .unwrap_err()
            .to_string()
            .contains("scalar binary value"));
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
