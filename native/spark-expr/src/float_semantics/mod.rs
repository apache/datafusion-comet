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

//! Spark's semantics for `-0.0` and NaN, in one place for all native expressions.
//!
//! Arrow orders floats by IEEE 754 total order: `-0.0` sorts below `0.0`, and NaNs compare by
//! their bits, so a NaN with the sign bit set sorts below `-Infinity`. Spark follows one of these
//! rules instead, each inherited from the Java API that a function's implementation calls:
//!
//! - SQL ordering, `SQLOrderingUtil.compareDoubles`: `-0.0` equals `0.0`, all NaNs are equal, and
//!   NaN sorts above every other value. Comparisons, sorting, `min`/`max` and the array functions
//!   that compare elements use it. See [`compare_floats`] and [`spark_comparator`].
//! - `NormalizeNaNAndZero`, which Spark applies to grouping, join and window partition keys:
//!   `-0.0` becomes `0.0` and every NaN becomes the canonical NaN. See [`normalize_float`],
//!   [`normalize_floats`] and [`normalize_nested_floats`].
//! - `java.lang.Double.equals`, which boxed keys such as those in `OpenHashSet` use: all NaNs are
//!   equal, but `-0.0` and `0.0` are distinct. See [`canonicalize_nan`].
//! - `Murmur3Hash` and `XxHash64`: `-0.0` hashes as `0.0`, and NaN as the canonical NaN. See
//!   [`hash_input`].
//!
//! A native expression must follow the rule of the Spark function it replaces, so build it from
//! these helpers rather than a local copy. Non-canonical NaNs are not a corner case: on x86-64
//! every NaN that arithmetic produces at run time, such as `sqrt(-1)`, has the sign bit set.

mod compare;
mod normalize;

pub use compare::spark_comparator;
pub use normalize::{
    has_float_leaf, normalize_floats, normalize_nested_floats, NormalizeNaNAndZero,
    NormalizeNestedFloats,
};

use num::Float;
use std::cmp::Ordering;

/// Spark's `NormalizeNaNAndZero`: every NaN becomes the canonical NaN and `-0.0` becomes `0.0`,
/// so two values that Spark's SQL ordering treats as equal also have the same bits.
#[inline]
pub fn normalize_float<T: Float>(v: T) -> T {
    if v.is_nan() {
        T::nan()
    } else if v == T::neg_zero() {
        T::zero()
    } else {
        v
    }
}

/// Canonicalizes NaN but keeps the sign of zero. This is the equality of a boxed
/// `java.lang.Double`, whose `equals` compares `doubleToLongBits`.
#[inline]
pub fn canonicalize_nan<T: Float>(v: T) -> T {
    if v.is_nan() {
        T::nan()
    } else {
        v
    }
}

/// Spark's SQL ordering, `SQLOrderingUtil.compareDoubles`: `-0.0` equals `0.0`, all NaNs are
/// equal, and NaN is greater than every other value, including positive infinity.
#[inline]
pub fn compare_floats<T: Float>(left: T, right: T) -> Ordering {
    // IEEE 754 already treats the two zeros as equal. Only a NaN leaves the values unordered.
    left.partial_cmp(&right)
        .unwrap_or_else(|| left.is_nan().cmp(&right.is_nan()))
}

/// The value that Spark's `Murmur3Hash` and `XxHash64` hash in place of a float. `-0.0` hashes as
/// `0.0`, so the two zeros hash alike.
///
/// Spark reads the other bits through `doubleToLongBits`, which also canonicalizes NaN. This does
/// not yet, so a NaN whose bits are not canonical hashes differently from Spark (#6385).
#[inline]
pub fn hash_input<T: Float>(v: T) -> T {
    if v == T::zero() {
        // `-0.0 == 0.0` in IEEE 754, so this catches negative zero as well.
        T::zero()
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A NaN with the sign bit set, which arithmetic produces on x86-64.
    const NEGATIVE_NAN: f64 = f64::from_bits(0xfff8_0000_0000_0000);
    /// A signaling NaN with a payload.
    const PAYLOAD_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0001);
    const NEGATIVE_NAN_F32: f32 = f32::from_bits(0xffc0_0000);

    #[test]
    fn normalize_float_folds_negative_zero_and_nan() {
        for (value, expected) in [
            (-0.0, 0.0),
            (0.0, 0.0),
            (NEGATIVE_NAN, f64::NAN),
            (PAYLOAD_NAN, f64::NAN),
            (f64::NEG_INFINITY, f64::NEG_INFINITY),
            (-1.5, -1.5),
        ] {
            assert_eq!(normalize_float(value).to_bits(), expected.to_bits());
        }
        assert_eq!(normalize_float(-0.0f32).to_bits(), 0.0f32.to_bits());
        assert_eq!(
            normalize_float(NEGATIVE_NAN_F32).to_bits(),
            f32::NAN.to_bits()
        );
    }

    #[test]
    fn canonicalize_nan_keeps_the_sign_of_zero() {
        for (value, expected) in [
            (-0.0, -0.0),
            (0.0, 0.0),
            (NEGATIVE_NAN, f64::NAN),
            (PAYLOAD_NAN, f64::NAN),
            (-1.5, -1.5),
        ] {
            assert_eq!(canonicalize_nan(value).to_bits(), expected.to_bits());
        }
        assert_eq!(canonicalize_nan(-0.0f32).to_bits(), (-0.0f32).to_bits());
        assert_eq!(
            canonicalize_nan(NEGATIVE_NAN_F32).to_bits(),
            f32::NAN.to_bits()
        );
    }

    #[test]
    fn compare_floats_matches_compare_doubles() {
        // Ascending under `compareDoubles`, with values that compare equal grouped together.
        let groups = [
            vec![f64::NEG_INFINITY],
            vec![-1.0],
            vec![-0.0, 0.0],
            vec![f64::MIN_POSITIVE],
            vec![f64::INFINITY],
            vec![f64::NAN, NEGATIVE_NAN, PAYLOAD_NAN],
        ];
        for (i, left) in groups.iter().enumerate() {
            for (j, right) in groups.iter().enumerate() {
                for (&l, &r) in left.iter().flat_map(|l| right.iter().map(move |r| (l, r))) {
                    assert_eq!(compare_floats(l, r), i.cmp(&j), "{l:?} vs {r:?}");
                }
            }
        }
        assert_eq!(
            compare_floats(NEGATIVE_NAN_F32, f32::INFINITY),
            Ordering::Greater
        );
        assert_eq!(compare_floats(-0.0f32, 0.0f32), Ordering::Equal);
    }

    #[test]
    fn hash_input_folds_negative_zero() {
        assert_eq!(hash_input(-0.0f64).to_bits(), 0);
        assert_eq!(hash_input(-0.0f32).to_bits(), 0);
        assert_eq!(hash_input(0.0f64).to_bits(), 0);
        assert_eq!(hash_input(-1.5f64), -1.5);
        assert_eq!(hash_input(f64::NEG_INFINITY), f64::NEG_INFINITY);
    }
}
