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

use arrow_schema::Field;
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use std::{any::Any, sync::Arc};

use crate::execution::datafusion::util::spark_bloom_filter;
use crate::execution::datafusion::util::spark_bloom_filter::SparkBloomFilter;
use arrow::array::ArrayRef;
use arrow_array::BinaryArray;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::{downcast_value, DataFusionError, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDFImpl, Signature,
};
use datafusion_physical_expr::expressions::Literal;

#[derive(Debug, Clone)]
pub struct BloomFilterAgg {
    name: String,
    signature: Signature,
    expr: Arc<dyn PhysicalExpr>,
    num_items: i32,
    num_bits: i32,
}

#[inline]
fn extract_i32_from_literal(expr: Arc<dyn PhysicalExpr>) -> i32 {
    match expr.as_any().downcast_ref::<Literal>().unwrap().value() {
        ScalarValue::Int64(scalar_value) => scalar_value.unwrap() as i32,
        _ => {
            unreachable!()
        }
    }
}

impl BloomFilterAgg {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        num_items: Arc<dyn PhysicalExpr>,
        num_bits: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        assert!(matches!(data_type, DataType::Binary));
        Self {
            name: name.into(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            expr,
            num_items: extract_i32_from_literal(num_items),
            num_bits: extract_i32_from_literal(num_bits),
        }
    }
}

impl AggregateUDFImpl for BloomFilterAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        Ok(Box::new(SparkBloomFilter::from((
            spark_bloom_filter::optimal_num_hash_functions(self.num_items, self.num_bits),
            self.num_bits,
        ))))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new("bits", DataType::Binary, false)])
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
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::Int64(Some(value)) = v {
                self.put_long(value);
            } else {
                unreachable!()
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.spark_serialization())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
        assert_eq!(states[0].len(), 1);
        let state_sv = downcast_value!(states[0], BinaryArray);
        self.merge_filter(state_sv.value_data());
        Ok(())
    }
}
