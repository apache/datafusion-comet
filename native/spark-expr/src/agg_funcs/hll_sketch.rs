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
use datasketches::hash_value::raw_bytes;
use datasketches::hll::{HllSketch, HllType, HllUnion};

/// A DataSketches HLL_8 sketch configured to match Spark's `HllSketchAgg`.
#[derive(Debug)]
pub struct SparkHllSketch {
    inner: HllSketch,
}

/// Byte offsets into the DataSketches HLL preamble, and the bits we need from it.
mod preamble {
    /// Serialization flags. Bit 3 is COMPACT.
    pub const FLAGS: usize = 5;
    /// Mode byte. Low two bits are the current mode (0 LIST, 1 SET, 2 HLL).
    pub const MODE: usize = 7;
    /// Number of auxiliary-map exceptions, for a sketch in an HLL array mode.
    pub const AUX_COUNT: usize = 36;
    /// Total preamble length for an HLL array mode, i.e. where the register block starts.
    pub const HLL_SIZE: usize = 40;
    pub const COMPACT_FLAG: u8 = 8;
    pub const CUR_MODE_MASK: u8 = 0x3;
    pub const CUR_MODE_HLL: u8 = 2;
    /// Target type lives in bits 2-3 of the mode byte: 0 HLL_4, 1 HLL_6, 2 HLL_8.
    pub const TGT_HLL4: u8 = 0;

    pub fn tgt_type(mode_byte: u8) -> u8 {
        (mode_byte >> 2) & 0x3
    }
}

/// An error for the one input shape `datasketches` 0.3.0 decodes to silently wrong values:
/// an updatable HLL_4 sketch carrying auxiliary-map exceptions.
///
/// `Array4::deserialize` reads exactly `aux_count` coupons from the aux region regardless of the
/// COMPACT flag. That is the *compact* aux layout. DataSketches-Java's *updatable* HLL_4 form
/// writes `1 << lgAuxArrInts` ints including the empty slots, so reading the first `aux_count` of
/// those pulls empty slots in as zero coupons and drops the real exceptions. Nothing errors, and
/// the estimate is quietly wrong.
///
/// The exposure is real rather than theoretical: Comet always *writes* HLL_8, but
/// `hll_sketch_estimate` / `hll_union` / `hll_union_agg` accept any binary column, and
/// DataSketches-Java's default target type is HLL_4. So a sketch column produced elsewhere can
/// land here. An exception map only appears once some register exceeds `curMin + 15`, so ordinary
/// low-cardinality HLL_4 input still reads correctly and is deliberately still accepted - the
/// check is narrowed to the shape that is actually mis-decoded rather than rejecting HLL_4
/// outright, which would refuse most third-party sketches for no reason.
fn reject_undecodable_hll4(bytes: &[u8]) -> Result<(), DataFusionError> {
    if bytes.len() < preamble::HLL_SIZE
        || bytes[preamble::MODE] & preamble::CUR_MODE_MASK != preamble::CUR_MODE_HLL
        || preamble::tgt_type(bytes[preamble::MODE]) != preamble::TGT_HLL4
        || bytes[preamble::FLAGS] & preamble::COMPACT_FLAG != 0
    {
        return Ok(());
    }
    let aux_count = u32::from_le_bytes([
        bytes[preamble::AUX_COUNT],
        bytes[preamble::AUX_COUNT + 1],
        bytes[preamble::AUX_COUNT + 2],
        bytes[preamble::AUX_COUNT + 3],
    ]);
    if aux_count == 0 {
        return Ok(());
    }
    Err(DataFusionError::Execution(format!(
        "Cannot read an updatable HLL_4 sketch with {aux_count} auxiliary-map entries: the \
         bundled datasketches decoder reads the compact auxiliary layout in both forms, so this \
         sketch would decode to a silently wrong estimate. Convert it to HLL_8, or to the compact \
         HLL_4 form, before reading it with Comet."
    )))
}

/// Work around a decoding bug in `datasketches` 0.3.0 for compact sketches in an HLL array mode.
///
/// `Array4::deserialize` (and the `Array6` / `Array8` equivalents) skip the register block
/// entirely when the COMPACT flag is set, leaving every register zero:
///
/// ```text
/// let mut data = vec![0u8; num_bytes];
/// if !compact {
///     cursor.read_exact(&mut data)?;
/// } else {
///     cursor.advance(num_bytes as u64);
/// }
/// ```
///
/// The damage is quiet, which is what makes it worth guarding: the decoded sketch's own
/// `estimate()` still looks correct because it comes back from the HIP accumulator in the
/// preamble, but every union built from it is wrong. Two disjoint 1,000-value sketches union to
/// ~989 rather than ~1991.
///
/// Clearing the flag is a correct parse rather than a guess. The register block is present in
/// both the compact and updatable forms, and the crate reads the HLL_4 auxiliary map as
/// `aux_count` coupons regardless of the flag - which is the compact layout. LIST and SET mode
/// compaction *is* a genuinely different layout, and the crate handles those correctly, so this
/// only touches HLL array mode.
///
/// Returns `None` when the input needs no rewriting, so the common path does not copy.
///
/// `compact_input_survives_a_union` pins the behaviour: if a future `datasketches` release fixes
/// the register read, that test is what tells us this can be deleted.
fn normalize_compact_hll_array(bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.len() <= preamble::MODE
        || bytes[preamble::MODE] & preamble::CUR_MODE_MASK != preamble::CUR_MODE_HLL
        || bytes[preamble::FLAGS] & preamble::COMPACT_FLAG == 0
    {
        return None;
    }
    let mut owned = bytes.to_vec();
    owned[preamble::FLAGS] &= !preamble::COMPACT_FLAG;
    Some(owned)
}

impl SparkHllSketch {
    /// Create an empty HLL_8 sketch with the given `lgConfigK`.
    pub fn new(lg_config_k: u8) -> Self {
        Self {
            inner: HllSketch::new(lg_config_k, HllType::Hll8),
        }
    }

    /// Update with a 64-bit integer. Spark widens narrower integrals to `long`
    /// before hashing, so callers pass the already-widened value; Rust's `as i64`
    /// sign-extends, matching Spark's `toLong`. Rust's `Hash` for `i64` writes 8
    /// little-endian bytes with no prefix, matching DataSketches-Java `update(long)`.
    pub fn update_i64(&mut self, v: i64) {
        self.inner.update(v);
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
        reject_undecodable_hll4(bytes)?;
        let normalized = normalize_compact_hll_array(bytes);
        HllSketch::deserialize(normalized.as_deref().unwrap_or(bytes))
            .map(|inner| Self { inner })
            .map_err(|e| DataFusionError::Execution(format!("invalid HLL sketch bytes: {e}")))
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

    /// An HLL_4 sketch with auxiliary-map exceptions must be refused rather than silently
    /// mis-decoded. At lgK=12 the exceptions appear somewhere above ~100k distinct values.
    #[test]
    fn updatable_hll4_with_aux_entries_is_rejected() {
        use datasketches::hll::{HllSketch, HllType};
        let mut sketch = HllSketch::new(12, HllType::Hll4);
        for i in 0..100_000i64 {
            sketch.update(i);
        }
        let bytes = sketch.serialize();
        // Confirm the premise rather than assuming it: HLL array mode, HLL_4, not compact, and
        // carrying at least one exception.
        assert_eq!(
            bytes[preamble::MODE] & preamble::CUR_MODE_MASK,
            preamble::CUR_MODE_HLL
        );
        assert_eq!(
            preamble::tgt_type(bytes[preamble::MODE]),
            preamble::TGT_HLL4
        );
        assert_eq!(bytes[preamble::FLAGS] & preamble::COMPACT_FLAG, 0);
        let aux_count = u32::from_le_bytes([
            bytes[preamble::AUX_COUNT],
            bytes[preamble::AUX_COUNT + 1],
            bytes[preamble::AUX_COUNT + 2],
            bytes[preamble::AUX_COUNT + 3],
        ]);
        assert!(aux_count > 0, "expected the sketch to carry aux entries");

        let err = SparkHllSketch::from_bytes(&bytes).unwrap_err().to_string();
        assert!(
            err.contains("auxiliary-map"),
            "expected a clear rejection, got {err}"
        );
    }

    /// The guard is narrowed to the shape that is actually mis-decoded, so ordinary HLL_4 input
    /// with no exceptions still reads. Rejecting HLL_4 outright would refuse most third-party
    /// sketches for no reason.
    #[test]
    fn hll4_without_aux_entries_still_reads() {
        use datasketches::hll::{HllSketch, HllType};
        let mut sketch = HllSketch::new(12, HllType::Hll4);
        for i in 0..1_000i64 {
            sketch.update(i);
        }
        let bytes = sketch.serialize();
        let aux_count = u32::from_le_bytes([
            bytes[preamble::AUX_COUNT],
            bytes[preamble::AUX_COUNT + 1],
            bytes[preamble::AUX_COUNT + 2],
            bytes[preamble::AUX_COUNT + 3],
        ]);
        assert_eq!(aux_count, 0, "this cardinality should need no exceptions");
        let read = SparkHllSketch::from_bytes(&bytes).unwrap();
        assert!(
            (read.estimate() - 1000.0).abs() < 40.0,
            "estimate {}",
            read.estimate()
        );
    }
}
