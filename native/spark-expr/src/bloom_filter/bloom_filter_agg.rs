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

use arrow::datatypes::{Field, FieldRef};
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use std::sync::Arc;

use crate::bloom_filter::spark_bloom_filter;
use crate::bloom_filter::spark_bloom_filter::{SparkBloomFilter, SparkBloomFilterVersion};

use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::StringArray;
use datafusion::common::{downcast_value, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{AggregateUDFImpl, Signature};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::Accumulator;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BloomFilterAgg {
    signature: Signature,
    num_items: i32,
    num_bits: i32,
    /// Output serialization version. Spark <= 4.0 only knows V1; Spark 4.1+'s
    /// `BloomFilter.create` defaults to V2, so the JVM serde sets this to V2 on
    /// 4.1+ to keep `bloom_filter_agg` byte-equivalent with Spark's aggregator.
    version: SparkBloomFilterVersion,
}

#[inline]
fn extract_i32_from_literal(expr: Arc<dyn PhysicalExpr>) -> i32 {
    match (*expr).downcast_ref::<Literal>().unwrap().value() {
        ScalarValue::Int64(scalar_value) => scalar_value.unwrap() as i32,
        _ => {
            unreachable!()
        }
    }
}

impl BloomFilterAgg {
    pub fn new(
        num_items: Arc<dyn PhysicalExpr>,
        num_bits: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        version: SparkBloomFilterVersion,
    ) -> Self {
        assert!(matches!(data_type, DataType::Binary));
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
            num_items: extract_i32_from_literal(num_items),
            num_bits: extract_i32_from_literal(num_bits),
            version,
        }
    }
}

impl AggregateUDFImpl for BloomFilterAgg {
    fn name(&self) -> &str {
        "bloom_filter_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkBloomFilter::new(
            self.version,
            spark_bloom_filter::optimal_num_hash_functions(self.num_items, self.num_bits),
            self.num_bits,
            // Spark's BloomFilterAggregate always uses BloomFilterImplV2.DEFAULT_SEED (= 0).
            0,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("bits", DataType::Binary, false))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }
}

impl Accumulator for SparkBloomFilter {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Hash strings in place rather than materializing a `ScalarValue::Utf8` per row.
        if let Some(strings) = arr.as_any().downcast_ref::<StringArray>() {
            for value in strings.iter().flatten() {
                self.put_binary(value.as_bytes());
            }
            return Ok(());
        }
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            match v {
                ScalarValue::Int8(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int16(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int32(Some(value)) => {
                    self.put_long(value as i64);
                }
                ScalarValue::Int64(Some(value)) => {
                    self.put_long(value);
                }
                ScalarValue::Utf8(Some(value)) => {
                    self.put_binary(value.as_bytes());
                }
                // Spark's BloomFilterAggregate.update ignores null inputs.
                ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Utf8(None) => {}
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "bloom_filter_agg received an unsupported input type: {other:?}"
                    )));
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Spark's BloomFilterAggregate.eval returns NULL when no bit is set,
        // i.e. the aggregate saw no non-null input. Mirror that here so an
        // empty-input bloom_filter_agg yields NULL rather than a serialized
        // empty filter.
        if self.cardinality() == 0 {
            return Ok(ScalarValue::Binary(None));
        }
        Ok(ScalarValue::Binary(Some(self.spark_serialization())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // There might be a more efficient way to do this by transmuting since calling state() on an
        // Accumulator is considered destructive.
        let state_sv = ScalarValue::Binary(Some(self.state_as_bytes()));
        Ok(vec![state_sv])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(
            states.len(),
            1,
            "Expect one element in 'states' but found {}",
            states.len()
        );
        let states = downcast_value!(states[0], BinaryArray);
        // A final aggregate can pass several partial states in one batch. Merge each
        // row's byte slice because the backing buffer can also contain other groups' states.
        for state in states.iter().flatten() {
            self.merge_filter(state)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_with_values(version: SparkBloomFilterVersion, values: &[i64]) -> SparkBloomFilter {
        let mut filter = SparkBloomFilter::new(version, 3, 1024, 0);
        for &value in values {
            filter.put_long(value);
        }
        filter
    }

    #[test]
    fn merge_batch_merges_each_row() {
        for version in [SparkBloomFilterVersion::V1, SparkBloomFilterVersion::V2] {
            let first = filter_with_values(version, &[11]).state_as_bytes();
            let second = filter_with_values(version, &[22]).state_as_bytes();
            let states =
                BinaryArray::from(vec![Some(first.as_slice()), None, Some(second.as_slice())]);
            let mut acc = filter_with_values(version, &[33]);
            acc.merge_batch(&[Arc::new(states)]).unwrap();
            assert_eq!(
                acc.state_as_bytes(),
                filter_with_values(version, &[11, 22, 33]).state_as_bytes()
            );
        }
    }

    #[test]
    fn merge_batch_respects_sliced_row_offsets() {
        for version in [SparkBloomFilterVersion::V1, SparkBloomFilterVersion::V2] {
            let excluded = filter_with_values(version, &[99]).state_as_bytes();
            let included = filter_with_values(version, &[11]).state_as_bytes();
            let states = BinaryArray::from(vec![
                excluded.as_slice(),
                included.as_slice(),
                excluded.as_slice(),
            ]);
            let mut acc = filter_with_values(version, &[]);
            acc.merge_batch(&[Arc::new(states.slice(1, 1))]).unwrap();
            assert_eq!(acc.state_as_bytes(), included);
        }
    }

    #[test]
    fn merge_batch_ignores_empty_and_null_states() {
        let mut acc = filter_with_values(SparkBloomFilterVersion::V1, &[11]);
        let expected = acc.state_as_bytes();
        for states in [
            BinaryArray::from(Vec::<Option<&[u8]>>::new()),
            BinaryArray::from(vec![None::<&[u8]>]),
        ] {
            acc.merge_batch(&[Arc::new(states)]).unwrap();
            assert_eq!(acc.state_as_bytes(), expected);
        }
    }

    #[test]
    fn merge_batch_propagates_incompatible_state_error() {
        let first = filter_with_values(SparkBloomFilterVersion::V1, &[11]).state_as_bytes();
        let second = filter_with_values(SparkBloomFilterVersion::V2, &[22]).state_as_bytes();
        let states = BinaryArray::from(vec![first.as_slice(), second.as_slice()]);
        let mut acc = filter_with_values(SparkBloomFilterVersion::V1, &[]);
        let error = acc.merge_batch(&[Arc::new(states)]).unwrap_err();
        assert!(error.to_string().contains("version mismatch"));
    }

    /// Spark's BloomFilterAggregate.eval returns NULL when the filter saw no
    /// non-null input (cardinality 0); an untouched accumulator must match.
    #[test]
    fn evaluate_empty_filter_yields_null() {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let mut acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Binary(None));
    }

    /// A filter with at least one set bit serializes to a non-null binary.
    #[test]
    fn evaluate_non_empty_filter_yields_binary() {
        let num_bits = 1024;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let mut acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        acc.put_long(42);
        assert!(matches!(
            acc.evaluate().unwrap(),
            ScalarValue::Binary(Some(_))
        ));
    }

    /// `size()` has to include the bit array the filter allocates up front, not only the struct.
    #[test]
    fn size_counts_bit_array() {
        let num_bits = 1 << 20;
        let num_hash = spark_bloom_filter::optimal_num_hash_functions(100, num_bits);
        let acc = SparkBloomFilter::new(SparkBloomFilterVersion::V1, num_hash, num_bits, 0);
        assert!(acc.size() >= num_bits as usize / 8);
    }
}
