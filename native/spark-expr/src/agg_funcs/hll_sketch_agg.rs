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

use crate::agg_funcs::hll_sketch::SparkHllSketch;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{downcast_value, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_plan::Accumulator;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HllSketchAgg {
    signature: Signature,
    lg_config_k: i32,
}

impl HllSketchAgg {
    pub fn new(lg_config_k: i32) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Binary,
                ],
                Volatility::Immutable,
            ),
            lg_config_k,
        }
    }
}

impl AggregateUDFImpl for HllSketchAgg {
    fn name(&self) -> &str {
        "hll_sketch_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(HllSketchAccumulator::new(self.lg_config_k as u8)))
    }
    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("sketch", DataType::Binary, true))])
    }
    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct HllSketchAccumulator {
    sketch: SparkHllSketch,
    saw_input: bool,
}

impl HllSketchAccumulator {
    pub fn new(lg_config_k: u8) -> Self {
        Self {
            sketch: SparkHllSketch::new(lg_config_k),
            saw_input: false,
        }
    }
}

impl Accumulator for HllSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|i| {
            match ScalarValue::try_from_array(arr, i)? {
                ScalarValue::Int8(Some(v)) => {
                    self.sketch.update_i64(v as i64);
                    self.saw_input = true;
                }
                ScalarValue::Int16(Some(v)) => {
                    self.sketch.update_i64(v as i64);
                    self.saw_input = true;
                }
                ScalarValue::Int32(Some(v)) => {
                    self.sketch.update_i64(v as i64);
                    self.saw_input = true;
                }
                ScalarValue::Int64(Some(v)) => {
                    self.sketch.update_i64(v);
                    self.saw_input = true;
                }
                ScalarValue::Utf8(Some(v)) => {
                    self.sketch.update_bytes(v.as_bytes());
                    self.saw_input = true;
                }
                ScalarValue::Binary(Some(v)) => {
                    self.sketch.update_bytes(&v);
                    self.saw_input = true;
                }
                // Spark's HllSketchAgg ignores null inputs.
                ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Utf8(None)
                | ScalarValue::Binary(None) => {}
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "hll_sketch_agg received an unsupported input type: {other:?}"
                    )))
                }
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Spark returns a non-null sketch even for empty groups only when it saw input;
        // an empty group yields NULL.
        if !self.saw_input {
            return Ok(ScalarValue::Binary(None));
        }
        Ok(ScalarValue::Binary(Some(self.sketch.to_sketch_bytes())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if !self.saw_input {
            return Ok(vec![ScalarValue::Binary(None)]);
        }
        Ok(vec![ScalarValue::Binary(Some(
            self.sketch.to_sketch_bytes(),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(states[0], BinaryArray);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let peer = SparkHllSketch::from_bytes(arr.value(i))?;
            // Merge peer into self by unioning; reuse SparkHllUnion via sketch merge.
            self.sketch.merge_sketch(&peer);
            self.saw_input = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::physical_plan::Accumulator;
    use std::sync::Arc;

    #[test]
    fn accumulates_and_estimates() {
        let mut acc = HllSketchAccumulator::new(12);
        let arr = Arc::new(Int64Array::from((0..1000i64).collect::<Vec<_>>()));
        acc.update_batch(&[arr]).unwrap();
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!("expected binary")
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert!((est - 1000).abs() <= 30, "estimate {est}");
    }
}
