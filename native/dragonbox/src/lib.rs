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

// Translated from C++ to Rust. The original C++ code can be found at
// https://github.com/jk-jeon/dragonbox and carries the following license:
//
// Copyright 2020-2021 Junekey Jeon
//
// The contents of this file may be used under the terms of
// the Apache License v2.0 with LLVM Exceptions.
//
//    (See accompanying file LICENSE-Apache or copy at
//     https://llvm.org/foundation/relicensing/LICENSE.txt)
//
// Alternatively, the contents of this file may be used under the terms of
// the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE-Boost or copy at
//     https://www.boost.org/LICENSE_1_0.txt)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

//! [![github]](https://github.com/dtolnay/dragonbox)&ensp;[![crates-io]](https://crates.io/crates/dragonbox)&ensp;[![docs-rs]](https://docs.rs/dragonbox)
//!
//! [github]: https://img.shields.io/badge/github-8da0cb?style=for-the-badge&labelColor=555555&logo=github
//! [crates-io]: https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&labelColor=555555&logo=rust
//! [docs-rs]: https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs
//!
//! <br>
//!
//! This crate contains a basic port of <https://github.com/jk-jeon/dragonbox>
//! to Rust for benchmarking purposes.
//!
//! Please see the upstream repo for an explanation of the approach and
//! comparison to the Ryū algorithm.
//!
//! # Example
//!
//! ```
//! fn main() {
//!     let mut buffer = datafusion_comet_dragonbox::Buffer::new();
//!     let printed = buffer.format(1.234);
//!     assert_eq!(printed, "1.234E0");
//! }
//! ```
//!
//! ## Performance (lower is better)
//!
//! ![performance](https://raw.githubusercontent.com/dtolnay/dragonbox/master/performance.png)

#![no_std]
#![doc(html_root_url = "https://docs.rs/dragonbox/0.1.10")]
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(
    clippy::bool_to_int_with_if,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::comparison_chain,
    clippy::doc_markdown,
    clippy::expl_impl_clone_on_copy,
    clippy::if_not_else,
    clippy::items_after_statements,
    clippy::manual_range_contains,
    clippy::must_use_candidate,
    clippy::needless_doctest_main,
    clippy::never_loop,
    clippy::ptr_as_ptr,
    clippy::shadow_unrelated,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::toplevel_ref_arg,
    clippy::unreadable_literal,
    clippy::unusual_byte_groupings
)]

mod buffer;
mod cache;
mod div;
mod log;
mod to_chars;
mod wuint;

use crate::buffer::Sealed;
use crate::cache::EntryTypeExt as _;
use core::mem::MaybeUninit;

/// Buffer correctly sized to hold the text representation of any floating point
/// value.
///
/// ## Example
///
/// ```
/// let mut buffer = datafusion_comet_dragonbox::Buffer::new();
/// let printed = buffer.format_finite(1.234);
/// assert_eq!(printed, "1.234E0");
/// ```
pub struct Buffer {
    bytes: [MaybeUninit<u8>; to_chars::MAX_OUTPUT_STRING_LENGTH],
}

/// A floating point number that can be written into a
/// [`dragonbox::Buffer`][Buffer].
///
/// This trait is sealed and cannot be implemented for types outside of the
/// `dragonbox` crate.
pub trait Float: Sealed {}

// IEEE754-binary64
const SIGNIFICAND_BITS: usize = 52;
const EXPONENT_BITS: usize = 11;
const MIN_EXPONENT: i32 = -1022;
const EXPONENT_BIAS: i32 = -1023;

// Defines an unsigned integer type that is large enough
// to carry a variable of type f64.
// Most of the operations will be done on this integer type.
type CarrierUint = u64;

// Defines a signed integer type for holding significand bits together with the
// sign bit.
type SignedSignificand = i64;

// Number of bits in the above unsigned integer type.
const CARRIER_BITS: usize = 64;

// Extract exponent bits from a bit pattern. The result must be aligned to the
// LSB so that there is no additional zero paddings on the right. This function
// does not do bias adjustment.
const fn extract_exponent_bits(u: CarrierUint) -> u32 {
    const EXPONENT_BITS_MASK: u32 = (1 << EXPONENT_BITS) - 1;
    (u >> SIGNIFICAND_BITS) as u32 & EXPONENT_BITS_MASK
}

// Remove the exponent bits and extract significand bits together with the sign
// bit.
const fn remove_exponent_bits(u: CarrierUint, exponent_bits: u32) -> SignedSignificand {
    (u ^ ((exponent_bits as CarrierUint) << SIGNIFICAND_BITS)) as SignedSignificand
}

// Shift the obtained signed significand bits to the left by 1 to remove the
// sign bit.
const fn remove_sign_bit_and_shift(s: SignedSignificand) -> CarrierUint {
    (s as CarrierUint) << 1
}

const fn is_nonzero(u: CarrierUint) -> bool {
    (u << 1) != 0
}

const fn is_negative(s: SignedSignificand) -> bool {
    s < 0
}

const fn has_even_significand_bits(s: SignedSignificand) -> bool {
    s % 2 == 0
}

const fn compute_power32<const K: u32>(a: u32) -> u32 {
    // assert!(k >= 0);
    let mut p = 1;
    let mut i = 0;
    while i < K {
        p *= a;
        i += 1;
    }
    p
}

const fn compute_power64<const K: u32>(a: u64) -> u64 {
    // assert!(k >= 0);
    let mut p = 1;
    let mut i = 0;
    while i < K {
        p *= a;
        i += 1;
    }
    p
}

const fn count_factors<const A: usize>(mut n: usize) -> u32 {
    // assert!(a > 1);
    let mut c = 0;
    while n % A == 0 {
        n /= A;
        c += 1;
    }
    c
}

fn break_rounding_tie(significand: &mut u64) {
    *significand = if *significand % 2 == 0 {
        *significand
    } else {
        *significand - 1
    };
}

// Compute floor(n / 10^N) for small N.
// Precondition: n <= 2^a * 5^b (a = max_pow2, b = max_pow5)
fn divide_by_pow10<const N: u32, const MAX_POW2: i32, const MAX_POW5: i32>(n: u64) -> u64 {
    // Ensure no overflow.
    assert!(MAX_POW2 + (log::floor_log2_pow10(MAX_POW5) - MAX_POW5) < 64);

    // Specialize for 64-bit division by 1000.
    // Ensure that the correctness condition is met.
    if N == 3
        && MAX_POW2 + (log::floor_log2_pow10(N as i32 + MAX_POW5) - (N as i32 + MAX_POW5)) < 70
    {
        wuint::umul128_upper64(n, 0x8312_6e97_8d4f_df3c) >> 9
    } else {
        struct Divisor<const N: u32>;
        impl<const N: u32> Divisor<N> {
            const VALUE: u64 = compute_power64::<N>(10);
        }
        n / Divisor::<N>::VALUE
    }
}

pub struct Decimal {
    pub significand: u64,
    pub exponent: i32,
}

const KAPPA: u32 = 2;

// The main algorithm assumes the input is a normal/subnormal finite number
fn compute_nearest_normal(
    two_fc: CarrierUint,
    exponent: i32,
    has_even_significand_bits: bool,
) -> Decimal {
    //////////////////////////////////////////////////////////////////////
    // Step 1: Schubfach multiplier calculation
    //////////////////////////////////////////////////////////////////////

    // Compute k and beta.
    let minus_k = log::floor_log10_pow2(exponent) - KAPPA as i32;
    let ref cache = unsafe { cache::get(-minus_k) };
    let beta_minus_1 = exponent + log::floor_log2_pow10(-minus_k);

    // Compute zi and deltai.
    // 10^kappa <= deltai < 10^(kappa + 1)
    let deltai = compute_delta(cache, beta_minus_1);
    let two_fr = two_fc | 1;
    let zi = compute_mul(two_fr << beta_minus_1, cache);

    //////////////////////////////////////////////////////////////////////
    // Step 2: Try larger divisor; remove trailing zeros if necessary
    //////////////////////////////////////////////////////////////////////

    const BIG_DIVISOR: u32 = compute_power32::<{ KAPPA + 1 }>(10);
    const SMALL_DIVISOR: u32 = compute_power32::<KAPPA>(10);

    // Using an upper bound on zi, we might be able to optimize the division
    // better than the compiler; we are computing zi / big_divisor here.
    let mut significand = divide_by_pow10::<
        { KAPPA + 1 },
        { SIGNIFICAND_BITS as i32 + KAPPA as i32 + 2 },
        { KAPPA as i32 + 1 },
    >(zi);
    let mut r = (zi - BIG_DIVISOR as u64 * significand) as u32;

    'small_divisor_case_label: loop {
        if r > deltai {
            break 'small_divisor_case_label;
        } else if r < deltai {
            // Exclude the right endpoint if necessary.
            if r == 0
                && !has_even_significand_bits
                && is_product_integer_fc_pm_half(two_fr, exponent, minus_k)
            {
                significand -= 1;
                r = BIG_DIVISOR;
                break 'small_divisor_case_label;
            }
        } else {
            // r == deltai; compare fractional parts.
            // Check conditions in the order different from the paper to take
            // advantage of short-circuiting.
            let two_fl = two_fc - 1;
            if (!has_even_significand_bits
                || !is_product_integer_fc_pm_half(two_fl, exponent, minus_k))
                && !compute_mul_parity(two_fl, cache, beta_minus_1)
            {
                break 'small_divisor_case_label;
            }
        }
        let exponent = minus_k + KAPPA as i32 + 1;

        return Decimal {
            significand,
            exponent,
        };
    }

    //////////////////////////////////////////////////////////////////////
    // Step 3: Find the significand with the smaller divisor
    //////////////////////////////////////////////////////////////////////

    significand *= 10;
    let exponent = minus_k + KAPPA as i32;

    let mut dist = r - (deltai / 2) + (SMALL_DIVISOR / 2);
    let approx_y_parity = ((dist ^ (SMALL_DIVISOR / 2)) & 1) != 0;

    // Is dist divisible by 10^kappa?
    let divisible_by_10_to_the_kappa = div::check_divisibility_and_divide_by_pow10(&mut dist);

    // Add dist / 10^kappa to the significand.
    significand += dist as CarrierUint;

    if divisible_by_10_to_the_kappa {
        // Check z^(f) >= epsilon^(f)
        // We have either yi == zi - epsiloni or yi == (zi - epsiloni) - 1,
        // where yi == zi - epsiloni if and only if z^(f) >= epsilon^(f)
        // Since there are only 2 possibilities, we only need to care about the parity.
        // Also, zi and r should have the same parity since the divisor
        // is an even number.
        if compute_mul_parity(two_fc, cache, beta_minus_1) != approx_y_parity {
            significand -= 1;
        } else {
            // If z^(f) >= epsilon^(f), we might have a tie
            // when z^(f) == epsilon^(f), or equivalently, when y is an integer.
            // For tie-to-up case, we can just choose the upper one.
            if is_product_integer_fc(two_fc, exponent, minus_k) {
                break_rounding_tie(&mut significand);
            }
        }
    }

    Decimal {
        significand,
        exponent,
    }
}

fn compute_nearest_shorter(exponent: i32) -> Decimal {
    // Compute k and beta.
    let minus_k = log::floor_log10_pow2_minus_log10_4_over_3(exponent);
    let beta_minus_1 = exponent + log::floor_log2_pow10(-minus_k);

    // Compute xi and zi.
    let ref cache = unsafe { cache::get(-minus_k) };

    let mut xi = compute_left_endpoint_for_shorter_interval_case(cache, beta_minus_1);
    let zi = compute_right_endpoint_for_shorter_interval_case(cache, beta_minus_1);

    // If the left endpoint is not an integer, increase it.
    if !is_left_endpoint_integer_shorter_interval(exponent) {
        xi += 1;
    }

    // Try bigger divisor.
    let significand = zi / 10;

    // If succeed, remove trailing zeros if necessary and return.
    if significand * 10 >= xi {
        return Decimal {
            significand,
            exponent: minus_k + 1,
        };
    }

    // Otherwise, compute the round-up of y.
    let mut significand = compute_round_up_for_shorter_interval_case(cache, beta_minus_1);
    let exponent = minus_k;

    // When tie occurs, choose one of them according to the rule.
    const SHORTER_INTERVAL_TIE_LOWER_THRESHOLD: i32 =
        -log::floor_log5_pow2_minus_log5_3(SIGNIFICAND_BITS as i32 + 4)
            - 2
            - SIGNIFICAND_BITS as i32;
    const SHORTER_INTERVAL_TIE_UPPER_THRESHOLD: i32 =
        -log::floor_log5_pow2(SIGNIFICAND_BITS as i32 + 2) - 2 - SIGNIFICAND_BITS as i32;
    if exponent >= SHORTER_INTERVAL_TIE_LOWER_THRESHOLD
        && exponent <= SHORTER_INTERVAL_TIE_UPPER_THRESHOLD
    {
        break_rounding_tie(&mut significand);
    } else if significand < xi {
        significand += 1;
    }

    Decimal {
        significand,
        exponent,
    }
}

fn compute_mul(u: CarrierUint, cache: &cache::EntryType) -> CarrierUint {
    wuint::umul192_upper64(u, *cache)
}

fn compute_delta(cache: &cache::EntryType, beta_minus_1: i32) -> u32 {
    (cache.high() >> ((CARRIER_BITS - 1) as i32 - beta_minus_1)) as u32
}

fn compute_mul_parity(two_f: CarrierUint, cache: &cache::EntryType, beta_minus_1: i32) -> bool {
    debug_assert!(beta_minus_1 >= 1);
    debug_assert!(beta_minus_1 < 64);

    ((wuint::umul192_middle64(two_f, *cache) >> (64 - beta_minus_1)) & 1) != 0
}

fn compute_left_endpoint_for_shorter_interval_case(
    cache: &cache::EntryType,
    beta_minus_1: i32,
) -> CarrierUint {
    (cache.high() - (cache.high() >> (SIGNIFICAND_BITS + 2)))
        >> ((CARRIER_BITS - SIGNIFICAND_BITS - 1) as i32 - beta_minus_1)
}

fn compute_right_endpoint_for_shorter_interval_case(
    cache: &cache::EntryType,
    beta_minus_1: i32,
) -> CarrierUint {
    (cache.high() + (cache.high() >> (SIGNIFICAND_BITS + 2)))
        >> ((CARRIER_BITS - SIGNIFICAND_BITS - 1) as i32 - beta_minus_1)
}

fn compute_round_up_for_shorter_interval_case(
    cache: &cache::EntryType,
    beta_minus_1: i32,
) -> CarrierUint {
    (cache.high() >> ((CARRIER_BITS - SIGNIFICAND_BITS - 2) as i32 - beta_minus_1)).div_ceil(2)
}

const MAX_POWER_OF_FACTOR_OF_5: i32 = log::floor_log5_pow2(SIGNIFICAND_BITS as i32 + 2);
const DIVISIBILITY_CHECK_BY_5_THRESHOLD: i32 =
    log::floor_log2_pow10(MAX_POWER_OF_FACTOR_OF_5 + KAPPA as i32 + 1);

fn is_product_integer_fc_pm_half(two_f: CarrierUint, exponent: i32, minus_k: i32) -> bool {
    const CASE_FC_PM_HALF_LOWER_THRESHOLD: i32 =
        -(KAPPA as i32) - log::floor_log5_pow2(KAPPA as i32);
    const CASE_FC_PM_HALF_UPPER_THRESHOLD: i32 = log::floor_log2_pow10(KAPPA as i32 + 1);

    // Case I: f = fc +- 1/2
    if exponent < CASE_FC_PM_HALF_LOWER_THRESHOLD {
        false
    }
    // For k >= 0
    else if exponent <= CASE_FC_PM_HALF_UPPER_THRESHOLD {
        true
    }
    // For k < 0
    else if exponent > DIVISIBILITY_CHECK_BY_5_THRESHOLD {
        false
    } else {
        unsafe {
            div::divisible_by_power_of_5::<{ MAX_POWER_OF_FACTOR_OF_5 as usize + 1 }>(
                two_f,
                minus_k as u32,
            )
        }
    }
}

fn is_product_integer_fc(two_f: CarrierUint, exponent: i32, minus_k: i32) -> bool {
    const CASE_FC_LOWER_THRESHOLD: i32 =
        -(KAPPA as i32) - 1 - log::floor_log5_pow2(KAPPA as i32 + 1);
    const CASE_FC_UPPER_THRESHOLD: i32 = log::floor_log2_pow10(KAPPA as i32 + 1);

    // Case II: f = fc + 1
    // Case III: f = fc
    // Exponent for 5 is negative
    if exponent > DIVISIBILITY_CHECK_BY_5_THRESHOLD {
        false
    } else if exponent > CASE_FC_UPPER_THRESHOLD {
        unsafe {
            div::divisible_by_power_of_5::<{ MAX_POWER_OF_FACTOR_OF_5 as usize + 1 }>(
                two_f,
                minus_k as u32,
            )
        }
    }
    // Both exponents are nonnegative
    else if exponent >= CASE_FC_LOWER_THRESHOLD {
        true
    }
    // Exponent for 2 is negative
    else {
        div::divisible_by_power_of_2(two_f, (minus_k - exponent + 1) as u32)
    }
}

const fn floor_log2(mut n: u64) -> i32 {
    let mut count = -1;
    while n != 0 {
        count += 1;
        n >>= 1;
    }
    count
}

fn is_left_endpoint_integer_shorter_interval(exponent: i32) -> bool {
    const CASE_SHORTER_INTERVAL_LEFT_ENDPOINT_LOWER_THRESHOLD: i32 = 2;
    const CASE_SHORTER_INTERVAL_LEFT_ENDPOINT_UPPER_THRESHOLD: i32 = 2 + floor_log2(
        compute_power64::<{ count_factors::<5>((1 << (SIGNIFICAND_BITS + 2)) - 1) + 1 }>(10) / 3,
    );
    exponent >= CASE_SHORTER_INTERVAL_LEFT_ENDPOINT_LOWER_THRESHOLD
        && exponent <= CASE_SHORTER_INTERVAL_LEFT_ENDPOINT_UPPER_THRESHOLD
}

pub fn to_decimal(x: f64) -> Decimal {
    let br = x.to_bits();
    let exponent_bits = extract_exponent_bits(br);
    let signed_significand_bits = remove_exponent_bits(br, exponent_bits);

    let mut two_fc = remove_sign_bit_and_shift(signed_significand_bits);
    let mut exponent = exponent_bits as i32;

    // Is the input a normal number?
    if exponent != 0 {
        exponent += EXPONENT_BIAS - SIGNIFICAND_BITS as i32;

        // Shorter interval case; proceed like Schubfach. One might think this
        // condition is wrong, since when exponent_bits == 1 and two_fc == 0,
        // the interval is actually regular. However, it turns out that this
        // seemingly wrong condition is actually fine, because the end result is
        // anyway the same.
        //
        // [binary32]
        // floor( (fc-1/2) * 2^e ) = 1.175'494'28... * 10^-38
        // floor( (fc-1/4) * 2^e ) = 1.175'494'31... * 10^-38
        // floor(    fc    * 2^e ) = 1.175'494'35... * 10^-38
        // floor( (fc+1/2) * 2^e ) = 1.175'494'42... * 10^-38
        //
        // Hence, shorter_interval_case will return 1.175'494'4 * 10^-38.
        // 1.175'494'3 * 10^-38 is also a correct shortest representation that
        // will be rejected if we assume shorter interval, but 1.175'494'4 *
        // 10^-38 is closer to the true value so it doesn't matter.
        //
        // [binary64]
        // floor( (fc-1/2) * 2^e ) = 2.225'073'858'507'201'13... * 10^-308
        // floor( (fc-1/4) * 2^e ) = 2.225'073'858'507'201'25... * 10^-308
        // floor(    fc    * 2^e ) = 2.225'073'858'507'201'38... * 10^-308
        // floor( (fc+1/2) * 2^e ) = 2.225'073'858'507'201'63... * 10^-308
        //
        // Hence, shorter_interval_case will return 2.225'073'858'507'201'4 * 10^-308.
        // This is indeed of the shortest length, and it is the unique one
        // closest to the true value among valid representations of the same
        // length.
        if two_fc == 0 {
            return compute_nearest_shorter(exponent);
        }

        two_fc |= 1 << (SIGNIFICAND_BITS + 1);
    }
    // Is the input a subnormal number?
    else {
        exponent = MIN_EXPONENT - SIGNIFICAND_BITS as i32;
    }

    compute_nearest_normal(
        two_fc,
        exponent,
        has_even_significand_bits(signed_significand_bits),
    )
}
