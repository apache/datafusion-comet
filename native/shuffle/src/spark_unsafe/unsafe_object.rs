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

use super::list::SparkUnsafeArray;
use super::map::SparkUnsafeMap;
use super::row::SparkUnsafeRow;
use datafusion_comet_common::bytes_to_i128;
use std::borrow::Cow;

const MAX_LONG_DIGITS: u8 = 18;

/// Decode `bytes` as UTF-8 the way Spark renders `StringType` -- `new String(bytes, UTF_8)` on the
/// JVM -- replacing each ill-formed sequence with a single `U+FFFD` and skipping the same number of
/// bytes the JDK's UTF-8 `CharsetDecoder` (action REPLACE) would. Valid UTF-8 is returned as a
/// zero-cost borrow.
///
/// This intentionally differs from `str::from_utf8_lossy` for surrogate-range three-byte sequences
/// (`ED A0..BF ..`, e.g. CESU-8 / Java modified-UTF-8 supplementary chars) and for some other
/// ill-formed multi-byte units: `from_utf8_lossy` follows the Unicode "maximal subpart" rule and
/// can emit one `U+FFFD` per byte, whereas the JDK collapses certain ill-formed units into a single
/// `U+FFFD`. Matching the JDK byte-for-byte means a Comet columnar shuffle of arbitrary bytes
/// renders identically to a Spark JVM shuffle. The per-class malformed lengths below
/// (E0/ED overlong & surrogate handling, F0/F4 range checks) match the observable replacement
/// behavior of the JDK UTF-8 decoder; they were determined from observed
/// `new String(bytes, UTF_8)` output, not by reviewing the OpenJDK source.
pub(crate) fn decode_utf8_spark_lossy(bytes: &[u8]) -> Cow<'_, str> {
    // Fast path: well-formed UTF-8 borrows with zero copy (the overwhelmingly common case).
    if let Ok(s) = std::str::from_utf8(bytes) {
        return Cow::Borrowed(s);
    }

    const RC: char = '\u{FFFD}';
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    while i < n {
        let b1 = bytes[i];
        if b1 < 0x80 {
            out.push(b1 as char);
            i += 1;
        } else if (0xC2..=0xDF).contains(&b1) {
            // 2-byte lead. Bad/absent continuation -> single FFFD, skip 1.
            if i + 1 < n && (bytes[i + 1] & 0xC0) == 0x80 {
                let cp = (((b1 as u32) & 0x1F) << 6) | ((bytes[i + 1] as u32) & 0x3F);
                out.push(char::from_u32(cp).unwrap());
                i += 2;
            } else {
                out.push(RC);
                i += 1;
            }
        } else if (0xE0..=0xEF).contains(&b1) {
            // 3-byte lead.
            if i + 1 >= n {
                out.push(RC); // truncated lead at EOF
                i = n;
            } else {
                let b2 = bytes[i + 1];
                if (b1 == 0xE0 && (b2 & 0xE0) == 0x80) || (b2 & 0xC0) != 0x80 {
                    // overlong (E0 80..9F) or b2 not a continuation -> skip 1
                    out.push(RC);
                    i += 1;
                } else if i + 2 >= n {
                    out.push(RC); // truncated after a valid b2 at EOF
                    i = n;
                } else {
                    let b3 = bytes[i + 2];
                    if (b3 & 0xC0) != 0x80 {
                        out.push(RC); // b3 not a continuation -> skip 2
                        i += 2;
                    } else {
                        let cp = (((b1 as u32) & 0x0F) << 12)
                            | (((b2 as u32) & 0x3F) << 6)
                            | ((b3 as u32) & 0x3F);
                        if (0xD800..=0xDFFF).contains(&cp) {
                            // surrogate (e.g. ED A0 80) -> JDK skips all 3, single FFFD
                            out.push(RC);
                            i += 3;
                        } else {
                            out.push(char::from_u32(cp).unwrap());
                            i += 3;
                        }
                    }
                }
            }
        } else if (0xF0..=0xF4).contains(&b1) {
            // 4-byte lead.
            if i + 1 >= n {
                out.push(RC);
                i = n;
            } else {
                let b2 = bytes[i + 1];
                if (b1 == 0xF0 && !(0x90..=0xBF).contains(&b2))
                    || (b1 == 0xF4 && (b2 & 0xF0) != 0x80)
                    || (b2 & 0xC0) != 0x80
                {
                    out.push(RC); // bad b2 -> skip 1
                    i += 1;
                } else if i + 2 >= n {
                    out.push(RC);
                    i = n;
                } else if (bytes[i + 2] & 0xC0) != 0x80 {
                    out.push(RC); // b3 not a continuation -> skip 2
                    i += 2;
                } else if i + 3 >= n {
                    out.push(RC);
                    i = n;
                } else if (bytes[i + 3] & 0xC0) != 0x80 {
                    out.push(RC); // b4 not a continuation -> skip 3
                    i += 3;
                } else {
                    let cp = (((b1 as u32) & 0x07) << 18)
                        | (((b2 as u32) & 0x3F) << 12)
                        | (((bytes[i + 2] as u32) & 0x3F) << 6)
                        | ((bytes[i + 3] as u32) & 0x3F);
                    out.push(char::from_u32(cp).unwrap());
                    i += 4;
                }
            }
        } else {
            // Lone continuation (0x80..0xBF), overlong 2-byte leads (0xC0/0xC1), or out-of-range
            // 4-byte leads (0xF5..0xFF): each is a single ill-formed byte -> skip 1.
            out.push(RC);
            i += 1;
        }
    }
    Cow::Owned(out)
}

/// A common trait for Spark Unsafe classes that can be used to access the underlying data,
/// e.g., `UnsafeRow` and `UnsafeArray`. This defines a set of methods that can be used to
/// access the underlying data with index.
///
/// # Safety
///
/// Implementations must ensure that:
/// - `get_row_addr()` returns a valid pointer to JVM-allocated memory
/// - `get_element_offset()` returns a valid pointer within the row/array data region
/// - The memory layout follows Spark's UnsafeRow/UnsafeArray format
/// - The memory remains valid for the lifetime of the object (guaranteed by JVM ownership)
///
/// All accessor methods (get_boolean, get_int, etc.) use unsafe pointer operations but are
/// safe to call as long as:
/// - The index is within bounds (caller's responsibility)
/// - The object was constructed from valid Spark UnsafeRow/UnsafeArray data
///
/// # Alignment
///
/// Primitive accessor methods are implemented separately for each type because they have
/// different alignment guarantees:
/// - `SparkUnsafeRow`: All field offsets are 8-byte aligned (bitset width is a multiple of 8,
///   and each field slot is 8 bytes), so accessors use aligned `ptr::read()`.
/// - `SparkUnsafeArray`: The array base address may be unaligned when nested within a row's
///   variable-length region, so accessors use `ptr::read_unaligned()`.
pub trait SparkUnsafeObject {
    /// Returns the address of the row.
    fn get_row_addr(&self) -> i64;

    /// Returns the offset of the element at the given index.
    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8;

    fn get_boolean(&self, index: usize) -> bool;
    fn get_byte(&self, index: usize) -> i8;
    fn get_short(&self, index: usize) -> i16;
    fn get_int(&self, index: usize) -> i32;
    fn get_long(&self, index: usize) -> i64;
    fn get_float(&self, index: usize) -> f32;
    fn get_double(&self, index: usize) -> f64;
    fn get_date(&self, index: usize) -> i32;
    fn get_timestamp(&self, index: usize) -> i64;

    /// Returns the offset and length of the element at the given index.
    #[inline]
    fn get_offset_and_len(&self, index: usize) -> (i32, i32) {
        let offset_and_size = self.get_long(index);
        let offset = (offset_and_size >> 32) as i32;
        let len = offset_and_size as i32;
        (offset, len)
    }

    /// Returns string value at the given index of the object.
    ///
    /// Spark's `UnsafeRow.getUTF8String` wraps the bytes via `UTF8String.fromAddress` with no
    /// UTF-8 validation, and Spark's `cast(BinaryType -> StringType)` is a zero-copy reinterpret
    /// that can leave arbitrary bytes in a `StringType` column. Strict `from_utf8(..).unwrap()`
    /// here panics on those rows even though Spark itself treats them as opaque. We use
    /// `from_utf8_lossy`: it returns the original `&str` borrow for valid UTF-8 (zero-cost) and a
    /// `String` with `U+FFFD` replacements for invalid bytes (defined behavior, no UB). This
    /// avoids `from_utf8_unchecked`, which would construct a `&str` from arbitrary bytes -- UB per
    /// the Rust reference, and would propagate into downstream Arrow ops that internally call
    /// `str::from_utf8_unchecked` on the buffer.
    ///
    /// We decode via [`decode_utf8_spark_lossy`] rather than `String::from_utf8_lossy` so the
    /// `U+FFFD` replacement granularity matches Spark's `new String(bytes, UTF_8)` EXACTLY,
    /// including surrogate-range three-byte sequences (`ED A0..BF ..`) where the two std libraries
    /// disagree -- so a Comet shuffle of arbitrary bytes renders identically to a Spark shuffle.
    fn get_string(&self, index: usize) -> Cow<'_, str> {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        debug_assert!(addr != 0, "get_string: null address at index {index}");
        debug_assert!(
            len >= 0,
            "get_string: negative length {len} at index {index}"
        );
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };
        decode_utf8_spark_lossy(slice)
    }

    /// Returns binary value at the given index of the object.
    fn get_binary(&self, index: usize) -> &[u8] {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        // SAFETY: addr points to valid binary data within the variable-length region.
        // Offset and length are read from the fixed-length portion of the row/array.
        debug_assert!(addr != 0, "get_binary: null address at index {index}");
        debug_assert!(
            len >= 0,
            "get_binary: negative length {len} at index {index}"
        );
        unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) }
    }

    /// Returns decimal value at the given index of the object.
    fn get_decimal(&self, index: usize, precision: u8) -> i128 {
        if precision <= MAX_LONG_DIGITS {
            self.get_long(index) as i128
        } else {
            let slice = self.get_binary(index);
            bytes_to_i128(slice)
        }
    }

    /// Returns struct value at the given index of the object.
    fn get_struct(&self, index: usize, num_fields: usize) -> SparkUnsafeRow {
        let (offset, len) = self.get_offset_and_len(index);
        let mut row = SparkUnsafeRow::new_with_num_fields(num_fields);
        row.point_to(self.get_row_addr() + offset as i64, len);

        row
    }

    /// Returns array value at the given index of the object.
    fn get_array(&self, index: usize) -> SparkUnsafeArray {
        let (offset, _) = self.get_offset_and_len(index);
        SparkUnsafeArray::new(self.get_row_addr() + offset as i64)
    }

    fn get_map(&self, index: usize) -> SparkUnsafeMap {
        let (offset, len) = self.get_offset_and_len(index);
        SparkUnsafeMap::new(self.get_row_addr() + offset as i64, len)
    }
}

/// Generates primitive accessor implementations for `SparkUnsafeObject`.
///
/// Uses `$read_method` to read typed values from raw pointers:
/// - `read` for aligned access (SparkUnsafeRow — all offsets are 8-byte aligned)
/// - `read_unaligned` for potentially unaligned access (SparkUnsafeArray)
macro_rules! impl_primitive_accessors {
    ($read_method:ident) => {
        #[inline]
        fn get_boolean(&self, index: usize) -> bool {
            let addr = self.get_element_offset(index, 1);
            debug_assert!(
                !addr.is_null(),
                "get_boolean: null pointer at index {index}"
            );
            // SAFETY: addr points to valid element data within the row/array region.
            unsafe { *addr != 0 }
        }

        #[inline]
        fn get_byte(&self, index: usize) -> i8 {
            let addr = self.get_element_offset(index, 1);
            debug_assert!(!addr.is_null(), "get_byte: null pointer at index {index}");
            // SAFETY: addr points to valid element data (1 byte) within the row/array region.
            unsafe { *(addr as *const i8) }
        }

        #[inline]
        fn get_short(&self, index: usize) -> i16 {
            let addr = self.get_element_offset(index, 2) as *const i16;
            debug_assert!(!addr.is_null(), "get_short: null pointer at index {index}");
            // SAFETY: addr points to valid element data (2 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_int(&self, index: usize) -> i32 {
            let addr = self.get_element_offset(index, 4) as *const i32;
            debug_assert!(!addr.is_null(), "get_int: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_long(&self, index: usize) -> i64 {
            let addr = self.get_element_offset(index, 8) as *const i64;
            debug_assert!(!addr.is_null(), "get_long: null pointer at index {index}");
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_float(&self, index: usize) -> f32 {
            let addr = self.get_element_offset(index, 4) as *const f32;
            debug_assert!(!addr.is_null(), "get_float: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_double(&self, index: usize) -> f64 {
            let addr = self.get_element_offset(index, 8) as *const f64;
            debug_assert!(!addr.is_null(), "get_double: null pointer at index {index}");
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_date(&self, index: usize) -> i32 {
            let addr = self.get_element_offset(index, 4) as *const i32;
            debug_assert!(!addr.is_null(), "get_date: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_timestamp(&self, index: usize) -> i64 {
            let addr = self.get_element_offset(index, 8) as *const i64;
            debug_assert!(
                !addr.is_null(),
                "get_timestamp: null pointer at index {index}"
            );
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }
    };
}
pub(crate) use impl_primitive_accessors;

#[cfg(test)]
mod utf8_lossy_tests {
    use super::decode_utf8_spark_lossy;
    use std::borrow::Cow;

    /// Oracle = JDK 17 `new String(bytes, StandardCharsets.UTF_8)` (the renderer Spark uses for
    /// StringType). Each row's expected output was verified against the JVM. The decoder must match
    /// it byte-for-byte -- including the surrogate-range case where `str::from_utf8_lossy` differs.
    #[test]
    fn matches_jvm_replacement_granularity() {
        let cases: &[(&[u8], &str)] = &[
            (&[0xFF, 0xFE, 0x41], "\u{FFFD}\u{FFFD}A"),
            (&[0x80, 0x42], "\u{FFFD}B"),
            (&[0xE0, 0x80], "\u{FFFD}\u{FFFD}"),
            (&[0xF0, 0x80, 0x80, 0x41], "\u{FFFD}\u{FFFD}\u{FFFD}A"),
            (&[0xC0, 0xAF], "\u{FFFD}\u{FFFD}"),
            // The parity case: Rust's from_utf8_lossy would give three U+FFFD here.
            (&[0xED, 0xA0, 0x80], "\u{FFFD}"),
            (
                &[0xF4, 0x90, 0x80, 0x80],
                "\u{FFFD}\u{FFFD}\u{FFFD}\u{FFFD}",
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(
                decode_utf8_spark_lossy(bytes),
                *expected,
                "bytes {bytes:02x?} should render like the JVM"
            );
        }
    }

    #[test]
    fn valid_utf8_is_borrowed_zero_copy() {
        let s = "café — 日本語 🦀";
        match decode_utf8_spark_lossy(s.as_bytes()) {
            Cow::Borrowed(b) => assert_eq!(b, s),
            Cow::Owned(_) => panic!("valid UTF-8 must borrow, not allocate"),
        }
    }

    #[test]
    fn valid_multibyte_around_invalid_bytes_decodes() {
        // 'a' | é (C3 A9) | stray 0xFF | 'b' | 🦀 (F0 9F A6 80) -> valid chars preserved, one FFFD.
        let mut bytes = vec![b'a'];
        bytes.extend_from_slice("é".as_bytes());
        bytes.push(0xFF);
        bytes.push(b'b');
        bytes.extend_from_slice("🦀".as_bytes());
        assert_eq!(decode_utf8_spark_lossy(&bytes), "aé\u{FFFD}b🦀");
    }
}
