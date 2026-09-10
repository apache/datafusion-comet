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

/// Default `lgMaxK` used by Spark's `new Union()` when constructing the empty
/// union returned for a group that never absorbed any sketch.
const DEFAULT_LG_K: u8 = 12;

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
        // Spark's HllUnionAgg is declared non-nullable: an empty/all-null group
        // still returns the serialized bytes of an empty `new Union()` (default
        // lgMaxK), which estimates to 0, never NULL.
        match &self.union {
            Some(u) => Ok(ScalarValue::Binary(Some(u.to_sketch_bytes()))),
            None => Ok(ScalarValue::Binary(Some(
                SparkHllUnion::new(DEFAULT_LG_K).to_sketch_bytes(),
            ))),
        }
    }
    fn size(&self) -> usize {
        // An HLL_8 sketch at lgConfigK=k can heap-allocate up to 1 << k bytes;
        // account for that so memory reservation reflects actual usage.
        std::mem::size_of_val(self)
            + self
                .seen_lg_config_k
                .map(|k| 1usize << k as usize)
                .unwrap_or(0)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Unlike `evaluate`, an empty partial emits NULL rather than an empty lgConfigK=12
        // sketch. `merge_batch` skips nulls, so the Final phase sees nothing at all from a
        // partition that absorbed nothing - which is the point: emitting a concrete
        // lgConfigK=12 sketch here would make it the first `lgConfigK` the Final accumulator
        // sees, and every real sketch at a different k would then fail the "Sketches have
        // different lgConfigK values" check. A partition with no input must not get a vote on
        // the union's k.
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

    /// Spark's `HllUnionAgg` is non-nullable: an empty/all-null group still
    /// produces the serialized bytes of an empty union (estimate 0), not NULL.
    #[test]
    fn empty_group_evaluates_to_empty_sketch_not_null() {
        let mut acc = HllUnionAccumulator::new(false);
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!("expected Binary(Some(_)) for an empty group, got NULL")
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert_eq!(est, 0, "empty union should estimate to 0, got {est}");
    }

    #[test]
    fn size_accounts_for_sketch_heap() {
        let mut a = SparkHllSketch::new(12);
        for i in 0..10000i64 {
            a.update_i64(i);
        }
        let arr = Arc::new(BinaryArray::from(vec![Some(
            a.to_sketch_bytes().as_slice(),
        )]));
        let mut acc = HllUnionAccumulator::new(false);
        acc.update_batch(&[arr]).unwrap();
        assert!(
            acc.size() > 1000,
            "size() should account for the sketch heap allocation, got {}",
            acc.size()
        );
    }

    /// An empty partial must emit NULL, not an empty lgConfigK=12 sketch. Emitting a concrete
    /// sketch made the empty partition the first `lgConfigK` the Final accumulator saw, so a
    /// partition holding only lgConfigK=10 sketches then failed the mismatch check.
    #[test]
    fn empty_partial_state_is_null() {
        let mut acc = HllUnionAccumulator::new(false);
        assert_eq!(
            acc.state().unwrap(),
            vec![ScalarValue::Binary(None)],
            "an empty partial must not contribute a sketch to the final merge"
        );
    }

    #[test]
    fn empty_partial_does_not_fix_the_final_lg_config_k() {
        // Partition A saw nothing; partition B saw only lgConfigK=10 sketches. Merging A's
        // state before B's used to abort with "Sketches have different lgConfigK values:
        // 12 and 10".
        let mut empty_partial = HllUnionAccumulator::new(false);
        let empty_state = empty_partial.state().unwrap();

        let mut k10 = SparkHllSketch::new(10);
        for i in 0..1000i64 {
            k10.update_i64(i);
        }
        let mut k10_partial = HllUnionAccumulator::new(false);
        k10_partial
            .update_batch(&[Arc::new(BinaryArray::from(vec![Some(
                k10.to_sketch_bytes().as_slice(),
            )]))])
            .unwrap();
        let k10_state = k10_partial.state().unwrap();

        let mut final_acc = HllUnionAccumulator::new(false);
        for state in [empty_state, k10_state] {
            let arrays: Vec<ArrayRef> = state
                .into_iter()
                .map(|sv| sv.to_array_of_size(1).unwrap())
                .collect();
            final_acc.merge_batch(&arrays).unwrap();
        }

        let ScalarValue::Binary(Some(bytes)) = final_acc.evaluate().unwrap() else {
            panic!("expected Binary(Some(_))")
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert!((est - 1000).abs() <= 40, "union estimate {est}");
    }

    /// A compact-form sketch has to survive a union. `datasketches` 0.3.0 drops the register
    /// block for compact HLL array modes, which leaves the decoded sketch's own estimate intact
    /// (it is restored from the HIP accumulator) but makes every union built from it wrong.
    #[test]
    fn compact_input_survives_a_union() {
        let mut a = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            a.update_i64(i);
        }
        let mut b = SparkHllSketch::new(12);
        for i in 1000..2000i64 {
            b.update_i64(i);
        }
        // Set the COMPACT flag, which is what a DataSketches `toCompactByteArray()` sketch
        // carries. For an HLL array mode the register block is present either way, so this is
        // the same bytes with a different flag.
        let compact = |s: &SparkHllSketch| {
            let mut v = s.to_sketch_bytes();
            v[5] |= 8;
            v
        };
        let arr = Arc::new(BinaryArray::from(vec![
            Some(compact(&a).as_slice()),
            Some(compact(&b).as_slice()),
        ]));
        let mut acc = HllUnionAccumulator::new(false);
        acc.update_batch(&[arr]).unwrap();

        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!("expected Binary(Some(_))")
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert!(
            (est - 2000).abs() <= 80,
            "union of two disjoint compact sketches estimated {est}, expected ~2000"
        );
    }
}
