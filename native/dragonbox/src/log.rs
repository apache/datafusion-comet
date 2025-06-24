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

////////////////////////////////////////////////////////////////////////////////////////
// Utilities for fast/constexpr log computation.
////////////////////////////////////////////////////////////////////////////////////////

const fn floor_shift(integer_part: u32, fractional_digits: u64, shift_amount: usize) -> i32 {
    //debug_assert!(shift_amount < 32);
    // Ensure no overflow
    //debug_assert!(shift_amount == 0 || integer_part < (1 << (32 - shift_amount)));

    if shift_amount == 0 {
        integer_part as i32
    } else {
        ((integer_part << shift_amount) | (fractional_digits >> (64 - shift_amount)) as u32) as i32
    }
}

// Compute floor(e * c - s).
const fn compute<
    const C_INTEGER_PART: u32,
    const C_FRACTIONAL_DIGITS: u64,
    const SHIFT_AMOUNT: usize,
    const MAX_EXPONENT: i32,
    const S_INTEGER_PART: u32,
    const S_FRACTIONAL_DIGITS: u64,
>(
    e: i32,
) -> i32 {
    //debug_assert!(e <= MAX_EXPONENT && e >= -MAX_EXPONENT);
    let c = floor_shift(C_INTEGER_PART, C_FRACTIONAL_DIGITS, SHIFT_AMOUNT);
    let s = floor_shift(S_INTEGER_PART, S_FRACTIONAL_DIGITS, SHIFT_AMOUNT);
    (e * c - s) >> SHIFT_AMOUNT
}

const LOG10_2_FRACTIONAL_DIGITS: u64 = 0x4d10_4d42_7de7_fbcc;
const LOG10_4_OVER_3_FRACTIONAL_DIGITS: u64 = 0x1ffb_fc2b_bc78_0375;
const FLOOR_LOG10_POW2_SHIFT_AMOUNT: usize = 22;
const FLOOR_LOG10_POW2_INPUT_LIMIT: i32 = 1700;
const FLOOR_LOG10_POW2_MINUS_LOG10_4_OVER_3_INPUT_LIMIT: i32 = 1700;

const LOG2_10_FRACTIONAL_DIGITS: u64 = 0x5269_e12f_346e_2bf9;
const FLOOR_LOG2_POW10_SHIFT_AMOUNT: usize = 19;
const FLOOR_LOG2_POW10_INPUT_LIMIT: i32 = 1233;

const LOG5_2_FRACTIONAL_DIGITS: u64 = 0x6e40_d1a4_143d_cb94;
const LOG5_3_FRACTIONAL_DIGITS: u64 = 0xaebf_4791_5d44_3b24;
const FLOOR_LOG5_POW2_SHIFT_AMOUNT: usize = 20;
const FLOOR_LOG5_POW2_INPUT_LIMIT: i32 = 1492;
const FLOOR_LOG5_POW2_MINUS_LOG5_3_INPUT_LIMIT: i32 = 2427;

pub(crate) const fn floor_log10_pow2(e: i32) -> i32 {
    compute::<
        0,
        LOG10_2_FRACTIONAL_DIGITS,
        FLOOR_LOG10_POW2_SHIFT_AMOUNT,
        FLOOR_LOG10_POW2_INPUT_LIMIT,
        0,
        0,
    >(e)
}

pub(crate) const fn floor_log2_pow10(e: i32) -> i32 {
    compute::<
        3,
        LOG2_10_FRACTIONAL_DIGITS,
        FLOOR_LOG2_POW10_SHIFT_AMOUNT,
        FLOOR_LOG2_POW10_INPUT_LIMIT,
        0,
        0,
    >(e)
}

pub(crate) const fn floor_log5_pow2(e: i32) -> i32 {
    compute::<
        0,
        LOG5_2_FRACTIONAL_DIGITS,
        FLOOR_LOG5_POW2_SHIFT_AMOUNT,
        FLOOR_LOG5_POW2_INPUT_LIMIT,
        0,
        0,
    >(e)
}

pub(crate) const fn floor_log5_pow2_minus_log5_3(e: i32) -> i32 {
    compute::<
        0,
        LOG5_2_FRACTIONAL_DIGITS,
        FLOOR_LOG5_POW2_SHIFT_AMOUNT,
        FLOOR_LOG5_POW2_MINUS_LOG5_3_INPUT_LIMIT,
        0,
        LOG5_3_FRACTIONAL_DIGITS,
    >(e)
}

pub(crate) const fn floor_log10_pow2_minus_log10_4_over_3(e: i32) -> i32 {
    compute::<
        0,
        LOG10_2_FRACTIONAL_DIGITS,
        FLOOR_LOG10_POW2_SHIFT_AMOUNT,
        FLOOR_LOG10_POW2_MINUS_LOG10_4_OVER_3_INPUT_LIMIT,
        0,
        LOG10_4_OVER_3_FRACTIONAL_DIGITS,
    >(e)
}
