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

use crate::{CarrierUint, CARRIER_BITS};

const fn modular_inverse<const A: CarrierUint>() -> CarrierUint {
    // By Euler's theorem, a^phi(2^n) == 1 (mod 2^n),
    // where phi(2^n) = 2^(n-1), so the modular inverse of a is
    // a^(2^(n-1) - 1) = a^(1 + 2 + 2^2 + ... + 2^(n-2)).
    let mut mod_inverse: CarrierUint = 1;
    let mut i = 1;
    while i < CARRIER_BITS {
        mod_inverse = mod_inverse.wrapping_mul(mod_inverse).wrapping_mul(A);
        i += 1;
    }
    mod_inverse
}

struct Table<const A: CarrierUint, const N: usize> {
    //assert(a % 2 != 0);
    //assert(N > 0);
    mod_inv: [CarrierUint; N],
    max_quotients: [CarrierUint; N],
}

impl<const A: CarrierUint, const N: usize> Table<A, N> {
    const TABLE: Self = {
        let mod_inverse = modular_inverse::<A>();
        let mut mod_inv = [0; N];
        let mut max_quotients = [0; N];
        let mut pow_of_mod_inverse: CarrierUint = 1;
        let mut pow_of_a = 1;
        let mut i = 0;
        while i < N {
            mod_inv[i] = pow_of_mod_inverse;
            max_quotients[i] = CarrierUint::MAX / pow_of_a;

            pow_of_mod_inverse = pow_of_mod_inverse.wrapping_mul(mod_inverse);
            pow_of_a *= A;
            i += 1;
        }
        Table {
            mod_inv,
            max_quotients,
        }
    };
}

pub(crate) unsafe fn divisible_by_power_of_5<const TABLE_SIZE: usize>(
    x: CarrierUint,
    exp: u32,
) -> bool {
    let table = &Table::<5, TABLE_SIZE>::TABLE;
    debug_assert!((exp as usize) < TABLE_SIZE);
    (x * *table.mod_inv.get_unchecked(exp as usize))
        <= *table.max_quotients.get_unchecked(exp as usize)
}

pub(crate) fn divisible_by_power_of_2(x: CarrierUint, exp: u32) -> bool {
    debug_assert!(exp >= 1);
    debug_assert!(x != 0);
    x.trailing_zeros() >= exp
}

// Replace n by floor(n / 10^N).
// Returns true if and only if n is divisible by 10^N.
// Precondition: n <= 10^(N+1)
pub(crate) fn check_divisibility_and_divide_by_pow10(n: &mut u32) -> bool {
    const N: u32 = 2;
    debug_assert!(*n <= crate::compute_power32::<{ N + 1 }>(10));

    struct Info;
    impl Info {
        const MAGIC_NUMBER: u32 = 0x147c29;
        const BITS_FOR_COMPARISON: i32 = 12;
        const THRESHOLD: u32 = 0xa3;
        const SHIFT_AMOUNT: i32 = 27;
    }

    *n *= Info::MAGIC_NUMBER;

    const COMPARISON_MASK: u32 = if Info::BITS_FOR_COMPARISON >= 32 {
        u32::MAX
    } else {
        ((1 << Info::BITS_FOR_COMPARISON) - 1) as u32
    };

    // The lowest N bits of (n & comparison_mask) must be zero, and
    // (n >> N) & comparison_mask must be at most threshold.
    let c = ((*n >> N) | (*n << (Info::BITS_FOR_COMPARISON as u32 - N))) & COMPARISON_MASK;

    *n >>= Info::SHIFT_AMOUNT;
    c <= Info::THRESHOLD
}
