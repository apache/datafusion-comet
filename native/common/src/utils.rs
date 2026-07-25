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

use std::borrow::Cow;

/// Converts a slice of bytes to i128. The bytes are serialized in big-endian order by
/// `BigInteger.toByteArray()` in Java.
pub fn bytes_to_i128(slice: &[u8]) -> i128 {
    let mut bytes = [0; 16];
    let mut i = 0;
    while i != 16 && i != slice.len() {
        bytes[i] = slice[slice.len() - 1 - i];
        i += 1;
    }

    // if the decimal is negative, we need to flip all the bits
    if (slice[0] as i8) < 0 {
        while i < 16 {
            bytes[i] = !bytes[i];
            i += 1;
        }
    }

    i128::from_le_bytes(bytes)
}

/// Decode `bytes` as UTF-8 the way Spark renders `StringType` -- `new String(bytes, UTF_8)` on the
/// JVM -- replacing each ill-formed sequence with a single `U+FFFD` and skipping the same number of
/// bytes the JDK's UTF-8 `CharsetDecoder` (action REPLACE) would. Valid UTF-8 is returned as a
/// zero-cost borrow.
///
/// This intentionally differs from `str::from_utf8_lossy` for surrogate-range three-byte sequences
/// (`ED A0..BF ..`, e.g. CESU-8 / Java modified-UTF-8 supplementary chars) and for some other
/// ill-formed multi-byte units: `from_utf8_lossy` follows the Unicode "maximal subpart" rule and
/// can emit one `U+FFFD` per byte, whereas the JDK collapses certain ill-formed units into a single
/// `U+FFFD`. Matching the JDK byte-for-byte means Comet renders arbitrary bytes identically to
/// Spark -- whether they arrive via a columnar shuffle or a `CAST(binary AS string)`. The
/// per-class malformed lengths below (E0/ED overlong & surrogate handling, F0/F4 range checks)
/// match the observable replacement behavior of the JDK UTF-8 decoder; they were determined from
/// observed `new String(bytes, UTF_8)` output, not by reviewing the OpenJDK source.
pub fn decode_utf8_spark_lossy(bytes: &[u8]) -> Cow<'_, str> {
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
                // guards above keep cp a valid scalar (2-byte range, never a surrogate)
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
                            // surrogate range excluded above -> cp is a valid scalar
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
                    // F0/F4 range guards above keep cp within U+10000..=U+10FFFF -> a valid scalar
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Oracle = JDK 17 `new String(bytes, StandardCharsets.UTF_8)` (the renderer Spark uses for
    /// StringType). Each row's expected output was verified against the JVM. The decoder must match
    /// it byte-for-byte -- including the surrogate-range case where `str::from_utf8_lossy` differs.
    #[test]
    fn decode_utf8_spark_lossy_matches_jvm_replacement_granularity() {
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
    fn decode_utf8_spark_lossy_valid_utf8_is_borrowed_zero_copy() {
        let s = "café — 日本語 🦀";
        match decode_utf8_spark_lossy(s.as_bytes()) {
            Cow::Borrowed(b) => assert_eq!(b, s),
            Cow::Owned(_) => panic!("valid UTF-8 must borrow, not allocate"),
        }
    }

    #[test]
    fn decode_utf8_spark_lossy_valid_multibyte_around_invalid_bytes_decodes() {
        // 'a' | é (C3 A9) | stray 0xFF | 'b' | 🦀 (F0 9F A6 80) -> valid chars preserved, one FFFD.
        let mut bytes = vec![b'a'];
        bytes.extend_from_slice("é".as_bytes());
        bytes.push(0xFF);
        bytes.push(b'b');
        bytes.extend_from_slice("🦀".as_bytes());
        assert_eq!(decode_utf8_spark_lossy(&bytes), "aé\u{FFFD}b🦀");
    }
}
