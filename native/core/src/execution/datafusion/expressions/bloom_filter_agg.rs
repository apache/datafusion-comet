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

use arrow_schema::{Field, Schema};
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use datafusion_physical_expr::NullState;
use std::{any::Any, sync::Arc};

use crate::execution::datafusion::util::spark_bloom_filter;
use crate::execution::datafusion::util::spark_bloom_filter::SparkBloomFilter;
use arrow::{
    array::{ArrayRef, AsArray, Float32Array, PrimitiveArray, PrimitiveBuilder, UInt32Array},
    datatypes::{ArrowNativeTypeOp, ArrowPrimitiveType, Float64Type, UInt32Type},
    record_batch::RecordBatch,
};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::*;
use datafusion_common::{cast::as_float64_array, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature,
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

impl BloomFilterAgg {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        num_items: Arc<dyn PhysicalExpr>,
        num_bits: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        assert!(matches!(data_type, DataType::Binary));
        let num_items = match num_items
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap()
            .value()
        {
            ScalarValue::Int64(scalar_value) => scalar_value.unwrap() as i32,
            _ => {
                unreachable!()
            }
        };
        let num_bits = match num_bits.as_any().downcast_ref::<Literal>().unwrap().value() {
            ScalarValue::Int64(scalar_value) => scalar_value.unwrap() as i32,
            _ => {
                unreachable!()
            }
        };
        Self {
            name: name.into(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            expr,
            num_items: num_items,
            num_bits: num_bits,
        }
    }
}

impl AggregateUDFImpl for BloomFilterAgg {
    /// We implement as_any so that we can downcast the AggregateUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "bloom_filter_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    /// This is the accumulator factory; DataFusion uses it to create new accumulators.
    ///
    /// This is the accumulator factory for row wise accumulation; Even when `GroupsAccumulator`
    /// is supported, DataFusion will use this row oriented
    /// accumulator when the aggregate function is used as a window function
    /// or when there are only aggregates (no GROUP BY columns) in the plan.
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkBloomFilter::from((
            spark_bloom_filter::optimal_num_hash_functions(self.num_items, self.num_bits),
            self.num_bits,
        ))))
    }

    /// This is the description of the state. accumulator's state() must match the types here.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new("bits", DataType::Binary, false)])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for SparkBloomFilter {
    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
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
                unreachable!("")
            }
            Ok(())
        })
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> Result<ScalarValue> {
        // TODO(Matt): Finish this.
        // let value = self.prod.powf(1.0 / self.n as f64);
        // Ok(ScalarValue::from(value))
        Ok(ScalarValue::from(0))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // TODO(Matt): Finish this.
        Ok(vec![ScalarValue::from(0), ScalarValue::from(0)])
    }

    // Merge the output of `Self::state()` from other instances of this accumulator
    // into this accumulator's state
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // TODO(Matt): Finish this.
        // if states.is_empty() {
        //     return Ok(());
        // }
        // let arr = &states[0];
        // (0..arr.len()).try_for_each(|index| {
        //     let v = states
        //         .iter()
        //         .map(|array| ScalarValue::try_from_array(array, index))
        //         .collect::<Result<Vec<_>>>()?;
        //     if let (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) = (&v[0], &v[1])
        //     {
        //         self.prod *= prod;
        //         self.n += n;
        //     } else {
        //         unreachable!("")
        //     }
        //     Ok(())
        // })
        Ok(())
    }
}
