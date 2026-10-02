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
//! these helpers rather than a local copy. Where an Arrow kernel sorts, row-encodes or hashes the
//! values, normalize them first: once `-0.0` is folded and NaN canonicalized, Arrow's total order
//! agrees with `compareDoubles`. Where Comet compares values itself, or has to return the original
//! bits as `array_min` does, use [`compare_floats`], [`float_lt`], [`float_gt`],
//! [`spark_comparator`] or [`spark_equality`]. Non-canonical NaNs are not a corner case: on x86-64
//! every NaN that arithmetic produces at run time, such as `sqrt(-1)`, has the sign bit set.

mod compare;
mod normalize;

pub use compare::{spark_comparator, spark_equality};
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
    // In this form a caller's `.is_eq()` compiles to the equality test alone, which a
    // `partial_cmp` with a NaN fallback does not.
    if left == right || (left.is_nan() && right.is_nan()) {
        Ordering::Equal
    } else if left > right || left.is_nan() {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

/// Whether `left` sorts before `right` in Spark's SQL ordering, the same as
/// `compare_floats(left, right).is_lt()`. As a single test it compiles to a well-predicted branch
/// in a scan for a minimum, where the three-way comparison is several times slower.
#[inline]
pub fn float_lt<T: Float>(left: T, right: T) -> bool {
    left < right || (!left.is_nan() && right.is_nan())
}

/// Whether `left` sorts after `right` in Spark's SQL ordering, the same as
/// `compare_floats(left, right).is_gt()`. See [`float_lt`].
#[inline]
pub fn float_gt<T: Float>(left: T, right: T) -> bool {
    left > right || (left.is_nan() && !right.is_nan())
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

/// A NaN with the sign bit set, which arithmetic produces on x86-64.
#[cfg(test)]
const NEGATIVE_NAN: f64 = f64::from_bits(0xfff8_0000_0000_0000);
/// A signaling NaN with a payload.
#[cfg(test)]
const PAYLOAD_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0001);

#[cfg(test)]
mod tests {
    use super::*;

    const NEGATIVE_NAN_F32: f32 = f32::from_bits(0xffc0_0000);

    #[test]
    fn per_value_rules() {
        // Each row: the input, then what `normalize_float`, `canonicalize_nan` and `hash_input`
        // return for it.
        let rows = [
            (-0.0, 0.0, -0.0, 0.0),
            (0.0, 0.0, 0.0, 0.0),
            (-1.5, -1.5, -1.5, -1.5),
            (
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ),
            // `hash_input` does not canonicalize NaN yet (#6385).
            (NEGATIVE_NAN, f64::NAN, f64::NAN, NEGATIVE_NAN),
            (PAYLOAD_NAN, f64::NAN, f64::NAN, PAYLOAD_NAN),
        ];
        for (value, normalized, canonical, hashed) in rows {
            assert_eq!(normalize_float(value).to_bits(), normalized.to_bits());
            assert_eq!(canonicalize_nan(value).to_bits(), canonical.to_bits());
            assert_eq!(hash_input(value).to_bits(), hashed.to_bits());
        }
        let rows = [
            (-0.0, 0.0, -0.0, 0.0),
            (NEGATIVE_NAN_F32, f32::NAN, f32::NAN, NEGATIVE_NAN_F32),
        ];
        for (value, normalized, canonical, hashed) in rows {
            assert_eq!(normalize_float(value).to_bits(), normalized.to_bits());
            assert_eq!(canonicalize_nan(value).to_bits(), canonical.to_bits());
            assert_eq!(hash_input(value).to_bits(), hashed.to_bits());
        }
    }

    #[test]
    fn comparisons_match_compare_doubles() {
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
                for &l in left {
                    for &r in right {
                        assert_eq!(compare_floats(l, r), i.cmp(&j), "{l:?} vs {r:?}");
                        assert_eq!(float_lt(l, r), i < j, "{l:?} < {r:?}");
                        assert_eq!(float_gt(l, r), i > j, "{l:?} > {r:?}");
                    }
                }
            }
        }
        assert_eq!(
            compare_floats(NEGATIVE_NAN_F32, f32::INFINITY),
            Ordering::Greater
        );
        assert_eq!(compare_floats(-0.0f32, 0.0f32), Ordering::Equal);
    }
}
