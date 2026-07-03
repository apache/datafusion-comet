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

use crate::agg_funcs::hll_sketch::{SparkHllSketch, SparkHllUnion};
use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{downcast_value, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_plan::Accumulator;
use std::sync::Arc;

// NOTE: matches bloom_filter_agg.rs for DataFusion 54.0.0 - no `as_any` method on
// AggregateUDFImpl, and PartialEq/Eq/Hash are required (DynEq/DynHash).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HllUnionAgg {
    signature: Signature,
    allow_different_lg_config_k: bool,
}

impl HllUnionAgg {
    pub fn new(allow_different_lg_config_k: bool) -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Binary], Volatility::Immutable),
            allow_different_lg_config_k,
        }
    }
}

impl AggregateUDFImpl for HllUnionAgg {
    fn name(&self) -> &str {
        "hll_union_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(HllUnionAccumulator::new(
            self.allow_different_lg_config_k,
        )))
    }
    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("sketch", DataType::Binary, true))])
    }
    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct HllUnionAccumulator {
    // Spark's HllUnionAgg defers creating the Union until the first sketch is seen,
    // then builds `new Union(sketch.getLgConfigK)` - so lgMaxK is NOT a fixed 12.
    union: Option<SparkHllUnion>,
    allow_different_lg_config_k: bool,
    seen_lg_config_k: Option<u8>,
}

impl HllUnionAccumulator {
    pub fn new(allow_different_lg_config_k: bool) -> Self {
        Self {
            union: None,
            allow_different_lg_config_k,
            seen_lg_config_k: None,
        }
    }

    fn absorb(&mut self, bytes: &[u8]) -> Result<()> {
        let sketch = SparkHllSketch::from_bytes(bytes)?;
        let k = sketch.lg_config_k();
        match self.seen_lg_config_k {
            None => {
                // Lazily instantiate the union from the first sketch's lgConfigK.
                self.seen_lg_config_k = Some(k);
                self.union = Some(SparkHllUnion::new(k));
            }
            Some(prev) if prev != k && !self.allow_different_lg_config_k => {
                return Err(DataFusionError::Execution(format!(
                    "Sketches have different lgConfigK values: {prev} and {k}. \
                     Set allowDifferentLgConfigK to true to enable unions of different lgConfigK."
                )));
            }
            _ => {}
        }
        self.union.as_mut().unwrap().merge(&sketch);
        Ok(())
    }
}

impl Accumulator for HllUnionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = downcast_value!(values[0], BinaryArray);
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.absorb(arr.value(i))?;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.union {
            Some(u) => Ok(ScalarValue::Binary(Some(u.to_sketch_bytes()))),
            None => Ok(ScalarValue::Binary(None)),
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match &self.union {
            Some(u) => Ok(vec![ScalarValue::Binary(Some(u.to_sketch_bytes()))]),
            None => Ok(vec![ScalarValue::Binary(None)]),
        }
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(states[0], BinaryArray);
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.absorb(arr.value(i))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg_funcs::hll_sketch::SparkHllSketch;
    use arrow::array::BinaryArray;
    use datafusion::physical_plan::Accumulator;
    use std::sync::Arc;

    #[test]
    fn unions_sketch_column() {
        let mut a = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            a.update_i64(i);
        }
        let mut b = SparkHllSketch::new(12);
        for i in 500..1500i64 {
            b.update_i64(i);
        }
        let arr = Arc::new(BinaryArray::from(vec![
            Some(a.to_sketch_bytes().as_slice()),
            Some(b.to_sketch_bytes().as_slice()),
        ]));
        let mut acc = HllUnionAccumulator::new(false);
        acc.update_batch(&[arr]).unwrap();
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!()
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert!((est - 1500).abs() <= 45, "union estimate {est}");
    }
}
