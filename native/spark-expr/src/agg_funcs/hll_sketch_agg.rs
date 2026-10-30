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

/// Input rows are hashed into a sketch. The first `merge_batch` turns that sketch into a union,
/// which merges every partial sketch from then on, as `HllUnionAccumulator` does. Spark's
/// `HllSketchAgg.merge` builds a new union for each merged row instead. With `datasketches` 0.3.0
/// that copies a dense accumulator's register array into the new union and rescans all
/// `2^lgConfigK` registers, so every merged row would cost as much as the whole array.
#[derive(Debug)]
pub enum HllSketchAccumulator {
    Sketch(SparkHllSketch),
    Union(SparkHllUnion),
}

impl HllSketchAccumulator {
    pub fn new(lg_config_k: u8) -> Self {
        Self::Sketch(SparkHllSketch::new(lg_config_k))
    }

    fn update_sketch(sketch: &mut SparkHllSketch, arr: &ArrayRef) -> Result<()> {
        // Downcast once per batch rather than going through `ScalarValue::try_from_array` per
        // row: for the string and binary cases that copies every value onto the heap only to
        // hash it and drop it again.
        match arr.data_type() {
            DataType::Int8 => Self::update_ints(sketch, as_primitive_array::<Int8Type>(arr)),
            DataType::Int16 => Self::update_ints(sketch, as_primitive_array::<Int16Type>(arr)),
            DataType::Int32 => Self::update_ints(sketch, as_primitive_array::<Int32Type>(arr)),
            DataType::Int64 => Self::update_ints(sketch, as_primitive_array::<Int64Type>(arr)),
            DataType::Utf8 => Self::update_byte_slices(sketch, downcast_value!(arr, StringArray)),
            DataType::Binary => Self::update_byte_slices(sketch, downcast_value!(arr, BinaryArray)),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "hll_sketch_agg received an unsupported input type: {other:?}"
                )))
            }
        }
        Ok(())
    }

    /// Spark widens every accepted integral to `long` before hashing, so all four widths
    /// funnel through the same `i64` update. Nulls are ignored, matching `HllSketchAgg`.
    fn update_ints<T>(sketch: &mut SparkHllSketch, arr: &PrimitiveArray<T>)
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i64>,
    {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                sketch.update_i64(arr.value(i).into());
            }
        }
    }

    /// StringType hashes its UTF-8 bytes and BinaryType its bytes directly, so both share
    /// this loop.
    fn update_byte_slices<T>(sketch: &mut SparkHllSketch, arr: &GenericByteArray<T>)
    where
        T: ByteArrayType,
        for<'a> &'a T::Native: AsRef<[u8]>,
    {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                sketch.update_bytes(arr.value(i).as_ref());
            }
        }
    }

    fn to_sketch_bytes(&self) -> Vec<u8> {
        match self {
            Self::Sketch(sketch) => sketch.to_sketch_bytes(),
            Self::Union(union) => union.to_sketch_bytes(),
        }
    }
}

impl Accumulator for HllSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        match self {
            Self::Sketch(sketch) => Self::update_sketch(sketch, &values[0]),
            // DataFusion gives an accumulator either rows or partial sketches. Rows that follow
            // a merge anyway reach the union through a sketch of their own.
            Self::Union(union) => {
                let mut sketch = SparkHllSketch::new(union.lg_max_k());
                Self::update_sketch(&mut sketch, &values[0])?;
                union.merge(&sketch);
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Spark's HllSketchAgg is declared non-nullable: an empty/all-null group
        // still returns a serialized empty sketch (which estimates to 0), never NULL.
        Ok(ScalarValue::Binary(Some(self.to_sketch_bytes())))
    }

    fn size(&self) -> usize {
        // What the sketch or union holds now, not the 1 << lgConfigK dense maximum: a group stays
        // in a few-dozen-byte coupon list until it has seen enough distinct values.
        let heap = match self {
            Self::Sketch(sketch) => sketch.heap_size(),
            Self::Union(union) => union.heap_size(),
        };
        std::mem::size_of_val(self) + heap
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.to_sketch_bytes()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(states[0], BinaryArray);
        if let Self::Sketch(sketch) = self {
            // Rows hashed before the first merge carry over into the union.
            let mut union = SparkHllUnion::new(sketch.lg_config_k());
            union.merge(sketch);
            *self = Self::Union(union);
        }
        let Self::Union(union) = self else {
            unreachable!("the sketch was replaced by a union above")
        };
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                union.merge(&SparkHllSketch::from_bytes(arr.value(i))?);
            }
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

    /// A Final aggregation hands a group its partial sketches over many `merge_batch` calls, and
    /// they all have to go into one union. After every call the accumulator holds exactly what a
    /// single `SparkHllUnion` fed the same sketches holds, in SET mode and once dense.
    #[test]
    fn merge_batch_keeps_one_union_across_calls() {
        let partials: Vec<Vec<u8>> = (0..40i64)
            .map(|p| {
                let mut sketch = SparkHllSketch::new(12);
                for v in p * 50..(p + 1) * 50 {
                    sketch.update_i64(v);
                }
                sketch.to_sketch_bytes()
            })
            .collect();
        let mut acc = HllSketchAccumulator::new(12);
        let mut union = SparkHllUnion::new(12);
        for chunk in partials.chunks(7) {
            acc.merge_batch(&[Arc::new(BinaryArray::from_iter_values(chunk))])
                .unwrap();
            for bytes in chunk {
                union.merge(&SparkHllSketch::from_bytes(bytes).unwrap());
            }
            assert_eq!(acc.to_sketch_bytes(), union.to_sketch_bytes());
        }
        // 2,000 distinct values are well past the 384 coupons a lgConfigK 12 sketch holds before
        // it moves to the register array.
        assert!(acc.size() >= 1 << 12, "size {}", acc.size());
        let est = crate::agg_funcs::estimate_from_bytes(&sketch_bytes(&mut acc)).unwrap();
        assert!((est - 2000).abs() <= 60, "estimate {est}");
    }

    /// DataFusion gives an accumulator either rows or partial sketches, but one that gets both
    /// still has to count all of them: the rows before its first merge, the merged sketch, and
    /// the rows after.
    #[test]
    fn rows_before_and_after_a_merge_all_count() {
        let ints = |values: std::ops::Range<i64>| -> ArrayRef {
            Arc::new(Int64Array::from_iter_values(values))
        };
        let mut peer = SparkHllSketch::new(12);
        for v in 1000..2000 {
            peer.update_i64(v);
        }
        let mut acc = HllSketchAccumulator::new(12);
        acc.update_batch(&[ints(0..1000)]).unwrap();
        acc.merge_batch(&[Arc::new(BinaryArray::from_iter_values([
            peer.to_sketch_bytes()
        ]))])
        .unwrap();
        acc.update_batch(&[ints(2000..3000)]).unwrap();
        let est = crate::agg_funcs::estimate_from_bytes(&sketch_bytes(&mut acc)).unwrap();
        assert!((est - 3000).abs() <= 90, "estimate {est}");
    }

    /// 64 singleton groups at lgConfigK=21, grouped through a real Partial/Final plan in a 16 MiB
    /// pool. Each group's sketch holds one coupon in an 8-slot LIST, 32 bytes, and Spark runs the
    /// same query without trouble. Charging every group the 2 MiB dense register array instead
    /// asks for 128 MiB, and the Final aggregation fails even with spilling.
    #[tokio::test]
    async fn singleton_groups_at_high_lg_config_k_fit_a_small_pool() {
        use arrow::array::RecordBatch;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::logical_expr::AggregateUDF;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(16 * 1024 * 1024, 1.0)
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().with_target_partitions(2),
            runtime,
        );
        ctx.register_udaf(AggregateUDF::new_from_impl(HllSketchAgg::new(21)));
        let ids: ArrayRef = Arc::new(Int64Array::from((0..64i64).collect::<Vec<_>>()));
        ctx.register_batch("t", RecordBatch::try_from_iter([("id", ids)]).unwrap())
            .unwrap();

        let batches = ctx
            .sql("SELECT id, hll_sketch_agg(id) FROM t GROUP BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 64);
    }
}
