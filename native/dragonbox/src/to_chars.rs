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

use core::ptr;

// sign(1) + significand(17) + decimal_point(1) + exp_marker(1) + exp_sign(1) + exp(3)
pub(crate) const MAX_OUTPUT_STRING_LENGTH: usize = 1 + 17 + 1 + 1 + 1 + 3;

pub(crate) unsafe fn to_chars(x: f64, mut buffer: *mut u8) -> *mut u8 {
    let br = x.to_bits();
    let exponent_bits = crate::extract_exponent_bits(br);
    let s = crate::remove_exponent_bits(br, exponent_bits);

    if crate::is_negative(s) {
        *buffer = b'-';
        buffer = buffer.add(1);
    }

    if crate::is_nonzero(br) {
        let result = crate::to_decimal(x);
        to_chars_detail(result.significand, result.exponent, buffer)
    } else {
        ptr::copy_nonoverlapping(b"0E0".as_ptr(), buffer, 3);
        buffer.add(3)
    }
}

#[rustfmt::skip]
static RADIX_100_TABLE: [u8; 200] = [
    b'0', b'0', b'0', b'1', b'0', b'2', b'0', b'3', b'0', b'4',
    b'0', b'5', b'0', b'6', b'0', b'7', b'0', b'8', b'0', b'9',
    b'1', b'0', b'1', b'1', b'1', b'2', b'1', b'3', b'1', b'4',
    b'1', b'5', b'1', b'6', b'1', b'7', b'1', b'8', b'1', b'9',
    b'2', b'0', b'2', b'1', b'2', b'2', b'2', b'3', b'2', b'4',
    b'2', b'5', b'2', b'6', b'2', b'7', b'2', b'8', b'2', b'9',
    b'3', b'0', b'3', b'1', b'3', b'2', b'3', b'3', b'3', b'4',
    b'3', b'5', b'3', b'6', b'3', b'7', b'3', b'8', b'3', b'9',
    b'4', b'0', b'4', b'1', b'4', b'2', b'4', b'3', b'4', b'4',
    b'4', b'5', b'4', b'6', b'4', b'7', b'4', b'8', b'4', b'9',
    b'5', b'0', b'5', b'1', b'5', b'2', b'5', b'3', b'5', b'4',
    b'5', b'5', b'5', b'6', b'5', b'7', b'5', b'8', b'5', b'9',
    b'6', b'0', b'6', b'1', b'6', b'2', b'6', b'3', b'6', b'4',
    b'6', b'5', b'6', b'6', b'6', b'7', b'6', b'8', b'6', b'9',
    b'7', b'0', b'7', b'1', b'7', b'2', b'7', b'3', b'7', b'4',
    b'7', b'5', b'7', b'6', b'7', b'7', b'7', b'8', b'7', b'9',
    b'8', b'0', b'8', b'1', b'8', b'2', b'8', b'3', b'8', b'4',
    b'8', b'5', b'8', b'6', b'8', b'7', b'8', b'8', b'8', b'9',
    b'9', b'0', b'9', b'1', b'9', b'2', b'9', b'3', b'9', b'4',
    b'9', b'5', b'9', b'6', b'9', b'7', b'9', b'8', b'9', b'9',
];

#[rustfmt::skip]
static TRAILING_ZERO_COUNT_TABLE: [i8; 100] = [
    2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

fn decimal_length_minus_1(v: u32) -> i32 {
    debug_assert!(v < 1000000000);
    if v >= 100000000 {
        8
    } else if v >= 10000000 {
        7
    } else if v >= 1000000 {
        6
    } else if v >= 100000 {
        5
    } else if v >= 10000 {
        4
    } else if v >= 1000 {
        3
    } else if v >= 100 {
        2
    } else if v >= 10 {
        1
    } else {
        0
    }
}

// Granlund-Montgomery style fast division
struct QuotientRemainderPair {
    quotient: u32,
    remainder: u32,
}

fn fast_div<const DIVISOR: u32, const MAX_PRECISION: u32, const ADDITIONAL_PRECISION: u32>(
    n: u32,
) -> QuotientRemainderPair {
    debug_assert!(MAX_PRECISION > 0 && MAX_PRECISION <= 32);
    debug_assert!(n < (1 << MAX_PRECISION));

    let left_end = ((1u32 << (MAX_PRECISION + ADDITIONAL_PRECISION)) + DIVISOR - 1) / DIVISOR;
    let right_end = ((1u32 << ADDITIONAL_PRECISION) * ((1 << MAX_PRECISION) + 1)) / DIVISOR;

    // Ensures sufficient precision.
    debug_assert!(left_end <= right_end);
    // Ensures no overflow.
    debug_assert!(left_end <= (1 << (32 - MAX_PRECISION)) as u32);

    let quotient = (n * left_end) >> (MAX_PRECISION + ADDITIONAL_PRECISION);
    let remainder = n - DIVISOR * quotient;
    QuotientRemainderPair {
        quotient,
        remainder,
    }
}

unsafe fn to_chars_detail(significand: u64, mut exponent: i32, mut buffer: *mut u8) -> *mut u8 {
    let mut s32: u32;
    let mut remaining_digits_minus_1: i32;
    let mut exponent_position: i32;
    let mut may_have_more_trailing_zeros = false;

    if significand >> 32 != 0 {
        // Since significand is at most 10^17, the quotient is at most 10^9, so
        // it fits inside 32-bit integer
        s32 = (significand / 1_0000_0000) as u32;
        let mut r = (significand as u32).wrapping_sub(s32.wrapping_mul(1_0000_0000));

        remaining_digits_minus_1 = decimal_length_minus_1(s32) + 8;
        exponent += remaining_digits_minus_1;
        exponent_position = remaining_digits_minus_1 + 2;

        if r != 0 {
            // Print 8 digits
            // `c = r % 1_0000` https://bugs.llvm.org/show_bug.cgi?id=38217
            let c = r - 1_0000 * (r / 1_0000);
            r /= 1_0000;

            // c1 = r / 100; c2 = r % 100;
            let QuotientRemainderPair {
                quotient: c1,
                remainder: c2,
            } = fast_div::<100, 14, 5>(r);
            // c3 = c / 100; c4 = c % 100;
            let QuotientRemainderPair {
                quotient: c3,
                remainder: c4,
            } = fast_div::<100, 14, 5>(c);

            'after_print_label: loop {
                'print_c1_label: loop {
                    'print_c2_label: loop {
                        'print_c3_label: loop {
                            'print_c4_label: loop {
                                let mut tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c4 as usize);
                                if tz == 0 {
                                    break 'print_c4_label;
                                } else if tz == 1 {
                                    *buffer.offset(remaining_digits_minus_1 as isize) =
                                        *RADIX_100_TABLE.get_unchecked(c4 as usize * 2);
                                    exponent_position -= 1;
                                    break 'print_c3_label;
                                }

                                tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c3 as usize);
                                if tz == 0 {
                                    exponent_position -= 2;
                                    break 'print_c3_label;
                                } else if tz == 1 {
                                    *buffer.offset(remaining_digits_minus_1 as isize - 2) =
                                        *RADIX_100_TABLE.get_unchecked(c3 as usize * 2);
                                    exponent_position -= 3;
                                    break 'print_c2_label;
                                }

                                tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c2 as usize);
                                if tz == 0 {
                                    exponent_position -= 4;
                                    break 'print_c2_label;
                                } else if tz == 1 {
                                    *buffer.offset(remaining_digits_minus_1 as isize - 4) =
                                        *RADIX_100_TABLE.get_unchecked(c2 as usize * 2);
                                    exponent_position -= 5;
                                    break 'print_c1_label;
                                }

                                tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c1 as usize);
                                if tz == 0 {
                                    exponent_position -= 6;
                                    break 'print_c1_label;
                                }
                                // We assumed r != 0, so c1 cannot be zero in this case.
                                debug_assert!(tz == 1);
                                *buffer.offset(remaining_digits_minus_1 as isize - 6) =
                                    *RADIX_100_TABLE.get_unchecked(c1 as usize * 2);
                                exponent_position -= 7;
                                break 'after_print_label;
                            }

                            ptr::copy_nonoverlapping(
                                RADIX_100_TABLE.as_ptr().add(c4 as usize * 2),
                                buffer.offset(remaining_digits_minus_1 as isize),
                                2,
                            );
                            break;
                        }

                        ptr::copy_nonoverlapping(
                            RADIX_100_TABLE.as_ptr().add(c3 as usize * 2),
                            buffer.offset(remaining_digits_minus_1 as isize - 2),
                            2,
                        );
                        break;
                    }

                    ptr::copy_nonoverlapping(
                        RADIX_100_TABLE.as_ptr().add(c2 as usize * 2),
                        buffer.offset(remaining_digits_minus_1 as isize - 4),
                        2,
                    );
                    break;
                }

                ptr::copy_nonoverlapping(
                    RADIX_100_TABLE.as_ptr().add(c1 as usize * 2),
                    buffer.offset(remaining_digits_minus_1 as isize - 6),
                    2,
                );
                break;
            }
        }
        // r != 0
        else {
            // r == 0
            exponent_position -= 8;
            may_have_more_trailing_zeros = true;
        }
        remaining_digits_minus_1 -= 8;
    } else {
        s32 = significand as u32;
        if s32 >= 10_0000_0000 {
            remaining_digits_minus_1 = 9;
        } else {
            remaining_digits_minus_1 = decimal_length_minus_1(s32);
        }
        exponent += remaining_digits_minus_1;
        exponent_position = remaining_digits_minus_1 + 2;
        may_have_more_trailing_zeros = true;
    }

    while remaining_digits_minus_1 >= 4 {
        // c = s32 % 1_0000` https://bugs.llvm.org/show_bug.cgi?id=38217
        let c = s32 - 1_0000 * (s32 / 1_0000);
        s32 /= 1_0000;

        // c1 = c / 100; c2 = c % 100;
        let QuotientRemainderPair {
            quotient: c1,
            remainder: c2,
        } = fast_div::<100, 14, 5>(c);

        'inside_loop_after_print_label: loop {
            'inside_loop_print_c1_label: loop {
                'inside_loop_print_c2_label: loop {
                    if may_have_more_trailing_zeros {
                        let mut tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c2 as usize);
                        if tz == 0 {
                            may_have_more_trailing_zeros = false;
                            break 'inside_loop_print_c2_label;
                        } else if tz == 1 {
                            may_have_more_trailing_zeros = false;
                            exponent_position -= 1;
                            *buffer.offset(remaining_digits_minus_1 as isize) =
                                *RADIX_100_TABLE.get_unchecked(c2 as usize * 2);
                            break 'inside_loop_print_c1_label;
                        }

                        tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c1 as usize);
                        if tz == 0 {
                            may_have_more_trailing_zeros = false;
                            exponent_position -= 2;
                            break 'inside_loop_print_c1_label;
                        } else if tz == 1 {
                            may_have_more_trailing_zeros = false;
                            exponent_position -= 3;
                            *buffer.offset(remaining_digits_minus_1 as isize - 2) =
                                *RADIX_100_TABLE.get_unchecked(c1 as usize * 2);
                            break 'inside_loop_after_print_label;
                        }
                        exponent_position -= 4;
                        break 'inside_loop_after_print_label;
                    }
                    break;
                }

                ptr::copy_nonoverlapping(
                    RADIX_100_TABLE.as_ptr().add(c2 as usize * 2),
                    buffer.offset(remaining_digits_minus_1 as isize),
                    2,
                );
                break;
            }

            ptr::copy_nonoverlapping(
                RADIX_100_TABLE.as_ptr().add(c1 as usize * 2),
                buffer.offset(remaining_digits_minus_1 as isize - 2),
                2,
            );
            break;
        }
        remaining_digits_minus_1 -= 4;
    }
    if remaining_digits_minus_1 >= 2 {
        // c1 = s32 / 100; c2 = s32 % 100;
        let QuotientRemainderPair {
            quotient: c1,
            remainder: c2,
        } = fast_div::<100, 14, 5>(s32);
        s32 = c1;

        if may_have_more_trailing_zeros {
            let tz = *TRAILING_ZERO_COUNT_TABLE.get_unchecked(c2 as usize);
            exponent_position -= tz as i32;
            if tz == 0 {
                ptr::copy_nonoverlapping(
                    RADIX_100_TABLE.as_ptr().add(c2 as usize * 2),
                    buffer.offset(remaining_digits_minus_1 as isize),
                    2,
                );
                may_have_more_trailing_zeros = false;
            } else if tz == 1 {
                *buffer.offset(remaining_digits_minus_1 as isize) =
                    *RADIX_100_TABLE.get_unchecked(c2 as usize * 2);
                may_have_more_trailing_zeros = false;
            }
        } else {
            ptr::copy_nonoverlapping(
                RADIX_100_TABLE.as_ptr().add(c2 as usize * 2),
                buffer.offset(remaining_digits_minus_1 as isize),
                2,
            );
        }

        remaining_digits_minus_1 -= 2;
    }
    if remaining_digits_minus_1 > 0 {
        debug_assert!(remaining_digits_minus_1 == 1);
        // d1 = s32 / 10; d2 = s32 % 10;
        let QuotientRemainderPair {
            quotient: d1,
            remainder: d2,
        } = fast_div::<10, 7, 3>(s32);

        *buffer = b'0' + d1 as u8;
        if may_have_more_trailing_zeros && d2 == 0 {
            buffer = buffer.add(1);
        } else {
            *buffer.add(1) = b'.';
            *buffer.add(2) = b'0' + d2 as u8;
            buffer = buffer.offset(exponent_position as isize);
        }
    } else {
        *buffer = b'0' + s32 as u8;

        if may_have_more_trailing_zeros {
            buffer = buffer.add(1);
        } else {
            *buffer.add(1) = b'.';
            buffer = buffer.offset(exponent_position as isize);
        }
    }

    // Print exponent and return
    if exponent < 0 {
        ptr::copy_nonoverlapping(b"E-".as_ptr(), buffer, 2);
        buffer = buffer.add(2);
        exponent = -exponent;
    } else {
        *buffer = b'E';
        buffer = buffer.add(1);
    }

    if exponent >= 100 {
        // d1 = exponent / 10; d2 = exponent % 10;
        let QuotientRemainderPair {
            quotient: d1,
            remainder: d2,
        } = fast_div::<10, 10, 3>(exponent as u32);
        ptr::copy_nonoverlapping(RADIX_100_TABLE.as_ptr().add(d1 as usize * 2), buffer, 2);
        *buffer.add(2) = b'0' + d2 as u8;
        buffer = buffer.add(3);
    } else if exponent >= 10 {
        ptr::copy_nonoverlapping(
            RADIX_100_TABLE.as_ptr().add(exponent as usize * 2),
            buffer,
            2,
        );
        buffer = buffer.add(2);
    } else {
        *buffer = b'0' + exponent as u8;
        buffer = buffer.add(1);
    }

    buffer
}
