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
use arrow::array::{as_primitive_array, GenericByteArray, PrimitiveArray, StringArray};
use arrow::datatypes::{
    ArrowPrimitiveType, ByteArrayType, DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type,
    Int8Type,
};
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
}

impl HllSketchAccumulator {
    pub fn new(lg_config_k: u8) -> Self {
        Self {
            sketch: SparkHllSketch::new(lg_config_k),
        }
    }

    /// Spark widens every accepted integral to `long` before hashing, so all four widths
    /// funnel through the same `i64` update. Nulls are ignored, matching `HllSketchAgg`.
    fn update_ints<T>(&mut self, arr: &PrimitiveArray<T>)
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i64>,
    {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.sketch.update_i64(arr.value(i).into());
            }
        }
    }

    /// StringType hashes its UTF-8 bytes and BinaryType its bytes directly, so both share
    /// this loop.
    fn update_byte_slices<T>(&mut self, arr: &GenericByteArray<T>)
    where
        T: ByteArrayType,
        for<'a> &'a T::Native: AsRef<[u8]>,
    {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.sketch.update_bytes(arr.value(i).as_ref());
            }
        }
    }
}

impl Accumulator for HllSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Downcast once per batch rather than going through `ScalarValue::try_from_array` per
        // row: for the string and binary cases that copies every value onto the heap only to
        // hash it and drop it again.
        match arr.data_type() {
            DataType::Int8 => self.update_ints(as_primitive_array::<Int8Type>(arr)),
            DataType::Int16 => self.update_ints(as_primitive_array::<Int16Type>(arr)),
            DataType::Int32 => self.update_ints(as_primitive_array::<Int32Type>(arr)),
            DataType::Int64 => self.update_ints(as_primitive_array::<Int64Type>(arr)),
            DataType::Utf8 => self.update_byte_slices(downcast_value!(arr, StringArray)),
            DataType::Binary => self.update_byte_slices(downcast_value!(arr, BinaryArray)),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "hll_sketch_agg received an unsupported input type: {other:?}"
                )))
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Spark's HllSketchAgg is declared non-nullable: an empty/all-null group
        // still returns a serialized empty sketch (which estimates to 0), never NULL.
        Ok(ScalarValue::Binary(Some(self.sketch.to_sketch_bytes())))
    }

    fn size(&self) -> usize {
        // An HLL_8 sketch at lgConfigK=k can heap-allocate up to 1 << k bytes;
        // account for that so memory reservation reflects actual usage.
        std::mem::size_of_val(self) + (1usize << self.sketch.lg_config_k() as usize)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
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
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, Int8Array};
    use datafusion::physical_plan::Accumulator;
    use std::sync::Arc;

    fn sketch_bytes(acc: &mut HllSketchAccumulator) -> Vec<u8> {
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!("expected binary")
        };
        bytes
    }

    #[test]
    fn accumulates_and_estimates() {
        let mut acc = HllSketchAccumulator::new(12);
        let arr = Arc::new(Int64Array::from((0..1000i64).collect::<Vec<_>>()));
        acc.update_batch(&[arr]).unwrap();
        let bytes = sketch_bytes(&mut acc);
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert!((est - 1000).abs() <= 30, "estimate {est}");
    }

    /// `update_batch` downcasts the whole array once instead of building a `ScalarValue` per
    /// row. Every accepted input type has to keep hashing exactly as before, so compare the
    /// accumulator's bytes against a sketch fed the same values directly - byte equality, not
    /// an error bound, since any change in the hashed bytes would move registers.
    #[test]
    fn every_input_type_hashes_the_same_as_a_direct_update() {
        // Narrow integrals are widened to i64 (sign-extending), so negatives matter here.
        let mut acc = HllSketchAccumulator::new(12);
        acc.update_batch(&[Arc::new(Int8Array::from(vec![
            Some(-128),
            None,
            Some(0),
            Some(127),
        ]))])
        .unwrap();
        let mut direct = SparkHllSketch::new(12);
        for v in [-128i64, 0, 127] {
            direct.update_i64(v);
        }
        assert_eq!(sketch_bytes(&mut acc), direct.to_sketch_bytes());

        let mut acc = HllSketchAccumulator::new(12);
        acc.update_batch(&[Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
        ]))])
        .unwrap();
        let mut direct = SparkHllSketch::new(12);
        for v in [i32::MIN as i64, i32::MAX as i64] {
            direct.update_i64(v);
        }
        assert_eq!(sketch_bytes(&mut acc), direct.to_sketch_bytes());

        // Strings hash their UTF-8 bytes; the empty string is skipped by both paths.
        let mut acc = HllSketchAccumulator::new(12);
        acc.update_batch(&[Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some(""),
            Some("héllo"),
        ]))])
        .unwrap();
        let mut direct = SparkHllSketch::new(12);
        for v in ["a", "", "héllo"] {
            direct.update_bytes(v.as_bytes());
        }
        assert_eq!(sketch_bytes(&mut acc), direct.to_sketch_bytes());

        let mut acc = HllSketchAccumulator::new(12);
        acc.update_batch(&[Arc::new(BinaryArray::from(vec![
            Some(&b"\x00\xff"[..]),
            None,
            Some(&b""[..]),
            Some(&b"xyz"[..]),
        ]))])
        .unwrap();
        let mut direct = SparkHllSketch::new(12);
        for v in [&b"\x00\xff"[..], &b""[..], &b"xyz"[..]] {
            direct.update_bytes(v);
        }
        assert_eq!(sketch_bytes(&mut acc), direct.to_sketch_bytes());
    }

    /// Spark's `HllSketchAgg` is non-nullable: an empty/all-null group still
    /// produces a serialized empty sketch (estimate 0), not NULL.
    #[test]
    fn empty_group_evaluates_to_empty_sketch_not_null() {
        let mut acc = HllSketchAccumulator::new(12);
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            panic!("expected Binary(Some(_)) for an empty group, got NULL")
        };
        let est = crate::agg_funcs::estimate_from_bytes(&bytes).unwrap();
        assert_eq!(est, 0, "empty sketch should estimate to 0, got {est}");
    }

    #[test]
    fn size_accounts_for_sketch_heap() {
        let mut acc = HllSketchAccumulator::new(12);
        let arr = Arc::new(Int64Array::from((0..10000i64).collect::<Vec<_>>()));
        acc.update_batch(&[arr]).unwrap();
        assert!(
            acc.size() > 1000,
            "size() should account for the sketch heap allocation, got {}",
            acc.size()
        );
    }
}
