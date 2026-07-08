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

//! Thin wrapper over the `datasketches` crate's HLL sketch, isolating all
//! crate-specific API so Comet's aggregate/scalar code depends on a stable
//! surface. Every sketch uses `HllType::Hll8` and DataSketches'
//! `DEFAULT_UPDATE_SEED` (9001), matching Spark's `HllSketchAgg`.
//!
//! Input hashing goes through the crate's `hash_value` wrappers
//! (`raw_bytes` for strings/binary without Rust's length prefix, `sign_extend`
//! for narrow integers) so the MurmurHash3-x64-128 input bytes are identical to
//! DataSketches-Java. This makes the sketches mutually readable with Spark.
//!
//! Note: the crate serializes List/Set (low-cardinality) modes in DataSketches
//! *compact* form, whereas Spark emits the *updatable* form. The bytes are
//! therefore not byte-identical to Spark's output for small inputs, but
//! DataSketches `deserialize` reads both forms, so estimates round-trip in both
//! directions. Comet must own both Partial and Final aggregation
//! (`supportsMixedPartialFinal = false`) so this compact intermediate is only
//! ever read back by Comet.

use datafusion::error::DataFusionError;
use datasketches::hash_value::{raw_bytes, sign_extend};
use datasketches::hll::{HllSketch, HllType, HllUnion};

/// A DataSketches HLL_8 sketch configured to match Spark's `HllSketchAgg`.
#[derive(Debug)]
pub struct SparkHllSketch {
    inner: HllSketch,
}

impl SparkHllSketch {
    /// Create an empty HLL_8 sketch with the given `lgConfigK`.
    pub fn new(lg_config_k: u8) -> Self {
        Self {
            inner: HllSketch::new(lg_config_k, HllType::Hll8),
        }
    }

    /// Update with a 64-bit integer. Spark widens narrower integrals to `long`
    /// before hashing; callers should pass the already-widened value here.
    /// Rust's `Hash` for `i64` writes 8 little-endian bytes with no prefix,
    /// matching DataSketches-Java `update(long)`.
    pub fn update_i64(&mut self, v: i64) {
        self.inner.update(v);
    }

    /// Update with a narrow signed integer, sign-extending to 64 bits exactly as
    /// Spark's `toLong` does before hashing.
    pub fn update_i32(&mut self, v: i32) {
        self.inner.update(sign_extend::from_i32(v));
    }
    pub fn update_i16(&mut self, v: i16) {
        self.inner.update(sign_extend::from_i16(v));
    }
    pub fn update_i8(&mut self, v: i8) {
        self.inner.update(sign_extend::from_i8(v));
    }

    /// Update with raw bytes (used for both StringType UTF-8 bytes and
    /// BinaryType), hashing without Rust's slice length prefix. Empty inputs are
    /// skipped, matching DataSketches (and Spark), which ignore empty values.
    pub fn update_bytes(&mut self, v: &[u8]) {
        if v.is_empty() {
            return;
        }
        self.inner.update(raw_bytes::from_slice(v));
    }

    /// Serialize to DataSketches bytes (compact for List/Set modes, full for HLL
    /// array modes). Readable by Spark's `hll_sketch_estimate` / `hll_union_agg`.
    pub fn to_sketch_bytes(&self) -> Vec<u8> {
        self.inner.serialize()
    }

    /// Deserialize a DataSketches sketch (either compact or updatable form).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DataFusionError> {
        HllSketch::deserialize(bytes)
            .map(|inner| Self { inner })
            .map_err(|e| DataFusionError::Internal(format!("invalid HLL sketch bytes: {e}")))
    }

    /// The configured `lgConfigK`.
    pub fn lg_config_k(&self) -> u8 {
        self.inner.lg_config_k()
    }

    /// Raw cardinality estimate (caller rounds to `i64` for Spark).
    pub fn estimate(&self) -> f64 {
        self.inner.estimate()
    }

    /// Merge another sketch into this one via a union, keeping HLL_8 output.
    pub fn merge_sketch(&mut self, other: &SparkHllSketch) {
        let mut u = HllUnion::new(self.lg_config_k());
        u.update(&self.inner);
        u.update(&other.inner);
        self.inner = u.to_sketch(HllType::Hll8);
    }
}

/// A DataSketches HLL union configured to match Spark's `HllUnionAgg`.
#[derive(Debug)]
pub struct SparkHllUnion {
    inner: HllUnion,
}

impl SparkHllUnion {
    /// Create an empty union with the given `lgMaxK` (Spark fixes this at 12).
    pub fn new(lg_max_k: u8) -> Self {
        Self {
            inner: HllUnion::new(lg_max_k),
        }
    }

    /// Merge a sketch into the union.
    pub fn merge(&mut self, sketch: &SparkHllSketch) {
        self.inner.update(&sketch.inner);
    }

    /// The union result as an HLL_8 sketch's serialized bytes.
    pub fn to_sketch_bytes(&self) -> Vec<u8> {
        self.inner.to_sketch(HllType::Hll8).serialize()
    }
}

/// Estimate the distinct count from serialized sketch bytes, rounded to the
/// nearest `i64` (Spark's `hll_sketch_estimate` returns a `Long`).
pub fn estimate_from_bytes(bytes: &[u8]) -> Result<i64, DataFusionError> {
    let sketch = SparkHllSketch::from_bytes(bytes)?;
    Ok(sketch.estimate().round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_roundtrips_and_estimates() {
        let mut s = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            s.update_i64(i);
        }
        let bytes = s.to_sketch_bytes();
        let est = estimate_from_bytes(&bytes).unwrap();
        assert!(
            (est - 1000).abs() <= 30,
            "estimate {est} not within 3% of 1000"
        );
    }

    #[test]
    fn union_merges_two_sketches() {
        let mut a = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            a.update_i64(i);
        }
        let mut b = SparkHllSketch::new(12);
        for i in 500..1500i64 {
            b.update_i64(i);
        }
        let mut u = SparkHllUnion::new(12);
        u.merge(&a);
        u.merge(&b);
        let est = estimate_from_bytes(&u.to_sketch_bytes()).unwrap();
        assert!(
            (est - 1500).abs() <= 45,
            "union estimate {est} not within 3% of 1500"
        );
    }

    /// A sketch built from raw bytes (StringType/BinaryType path) round-trips and
    /// estimates. Empty inputs are skipped, so they do not affect the estimate.
    #[test]
    fn byte_input_roundtrips_and_estimates() {
        let mut s = SparkHllSketch::new(12);
        for i in 0..1000i64 {
            s.update_bytes(format!("val-{i}").as_bytes());
        }
        s.update_bytes(b""); // skipped, no effect
        let est = estimate_from_bytes(&s.to_sketch_bytes()).unwrap();
        assert!(
            (est - 1000).abs() <= 30,
            "estimate {est} not within 3% of 1000"
        );
    }

    /// Cross-engine regression guard: `testdata/hll_sketch_spark_lgk12.bin` was
    /// produced by Spark 3.5's `hll_sketch_agg(id)` over `range(0, 1000)`. Comet
    /// must read it and estimate the distinct count, proving the crate's
    /// serialization stays DataSketches-Java compatible across crate upgrades.
    /// (For this HLL_8 input the Comet-produced bytes are byte-identical to
    /// Spark's; low-cardinality List/Set sketches differ in bytes but remain
    /// mutually readable.)
    #[test]
    fn reads_spark_produced_sketch() {
        let bytes = include_bytes!("testdata/hll_sketch_spark_lgk12.bin");
        let est = estimate_from_bytes(bytes).unwrap();
        assert!(
            (est - 1000).abs() <= 30,
            "estimate {est} of Spark-produced sketch not within 3% of 1000"
        );
    }
}
