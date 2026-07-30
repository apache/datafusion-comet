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

//! Whitespace trimming for casts from string.
//!
//! Spark's string casts do not all agree on what "whitespace" means. There are exactly two
//! regimes, and neither of them matches Rust's `str::trim` (which trims Unicode whitespace) or
//! `<[u8]>::trim_ascii` (which omits `0x0B`):
//!
//! | Regime                      | Trimmed bytes            | Cast targets                                               |
//! |-----------------------------|--------------------------|------------------------------------------------------------|
//! | [`trim_all`]                | `0x00`-`0x20` and `0x7F` | boolean, byte, short, int, long, date, timestamp, timestamp_ntz |
//! | [`trim_java_string`]        | `0x00`-`0x20`            | float, double, decimal                                     |
//!
//! Crucially, **neither regime trims any non-ASCII whitespace**. `U+0085`, `U+00A0`, `U+1680`,
//! `U+2000`-`U+200A`, `U+2028`, `U+2029`, `U+202F`, `U+205F` and `U+3000` all leave Spark
//! returning NULL (or raising under ANSI) for every cast target, so using `str::trim` here
//! silently produces a value where Spark produces none.
//!
//! The two regimes differ only in `0x7F` (DELETE), which the `trimAll` set removes and the
//! `String.trim` set does not. That single byte is why a shared helper cannot be applied
//! uniformly: trimming it in the float/double/decimal paths would introduce a new divergence.
//!
//! Both helpers can slice on byte offsets without breaking UTF-8 because every byte either
//! regime trims is ASCII, and a byte below `0x80` never appears inside a multi-byte sequence.

/// True for the bytes trimmed by `org.apache.spark.unsafe.types.UTF8String.trimAll`, i.e. the
/// bytes `b` for which `Character.isWhitespace(b) || Character.isISOControl(b)` holds.
///
/// `isWhitespace` covers `0x09`-`0x0D`, `0x1C`-`0x1F` and `0x20`; `isISOControl` covers
/// `0x00`-`0x1F` and `0x7F`; the union is `0x00`-`0x20` plus `0x7F`. Spark widens a *signed*
/// `byte` into the `int` overload, so bytes `0x80`-`0xFF` arrive negative and are never trimmed.
#[inline]
pub(crate) const fn is_whitespace_or_iso_control(b: u8) -> bool {
    b <= 0x20 || b == 0x7F
}

/// True for the bytes trimmed by `java.lang.String.trim`, which drops any char `<= U+0020`.
///
/// A char above `U+0020` always encodes to bytes `>= 0x80` in UTF-8, so testing bytes rather
/// than chars gives the same answer.
#[inline]
pub(crate) const fn is_java_trim_byte(b: u8) -> bool {
    b <= 0x20
}

/// Trims the `UTF8String.trimAll` byte set (`0x00`-`0x20` and `0x7F`) from both ends.
///
/// This is the trim used by `CAST(string AS boolean)`, the integral casts, and the datetime
/// casts. See the [module docs](self) for why the other targets need [`trim_java_string`].
#[inline]
pub(crate) fn trim_all(s: &str) -> &str {
    let (start, end) = trim_range(s.as_bytes(), is_whitespace_or_iso_control);
    &s[start..end]
}

/// [`trim_all`] over a byte slice, for parsers that already work on bytes.
#[inline]
pub(crate) fn trim_all_bytes(bytes: &[u8]) -> &[u8] {
    let (start, end) = trim_range(bytes, is_whitespace_or_iso_control);
    &bytes[start..end]
}

/// Trims the `java.lang.String.trim` byte set (`0x00`-`0x20`, keeping `0x7F`) from both ends.
///
/// This is the trim used by `CAST(string AS float/double)` (via `Double.parseDouble`, which
/// calls `String.trim` before parsing) and by `CAST(string AS decimal)` (via
/// `Decimal.stringToJavaBigDecimal`, which does `str.toString.trim`).
#[inline]
pub(crate) fn trim_java_string(s: &str) -> &str {
    let (start, end) = trim_range(s.as_bytes(), is_java_trim_byte);
    &s[start..end]
}

/// Byte offsets of `bytes` with every leading and trailing byte matching `trimmed` removed.
///
/// Slicing a `&str` on the returned offsets cannot panic as long as `trimmed` only accepts ASCII
/// bytes, since those never appear inside a multi-byte UTF-8 sequence.
#[inline]
fn trim_range(bytes: &[u8], trimmed: fn(u8) -> bool) -> (usize, usize) {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && trimmed(bytes[start]) {
        start += 1;
    }
    while end > start && trimmed(bytes[end - 1]) {
        end -= 1;
    }
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every byte the `trimAll` regime trims, as derived from Java's `Character.isWhitespace`
    /// and `Character.isISOControl` rather than from the implementation under test.
    fn expected_trim_all_bytes() -> Vec<u8> {
        (0..=u8::MAX)
            .filter(|&b| {
                let is_whitespace = matches!(b, 0x09..=0x0D | 0x1C..=0x1F | 0x20);
                let is_iso_control = matches!(b, 0x00..=0x1F | 0x7F);
                is_whitespace || is_iso_control
            })
            .collect()
    }

    #[test]
    fn trim_all_matches_spark_byte_set() {
        for b in 0..=u8::MAX {
            let expected = expected_trim_all_bytes().contains(&b);
            assert_eq!(
                is_whitespace_or_iso_control(b),
                expected,
                "byte 0x{b:02X} classified incorrectly"
            );
        }
    }

    #[test]
    fn java_trim_set_is_trim_all_without_delete() {
        for b in 0..=u8::MAX {
            assert_eq!(is_java_trim_byte(b), b <= 0x20, "byte 0x{b:02X}");
        }
        // The two regimes differ in exactly one byte: DELETE.
        let differing: Vec<u8> = (0..=u8::MAX)
            .filter(|&b| is_whitespace_or_iso_control(b) != is_java_trim_byte(b))
            .collect();
        assert_eq!(differing, vec![0x7F]);
    }

    /// `<[u8]>::trim_ascii` and `str::trim` are both wrong here; pin down how, so that a future
    /// change back to either is caught.
    #[test]
    fn rust_builtins_disagree_with_spark() {
        // trim_ascii omits the vertical tab, which Spark trims.
        assert!(is_whitespace_or_iso_control(0x0B));
        assert!(!0x0Bu8.is_ascii_whitespace());
        // str::trim removes non-ASCII whitespace, which Spark never trims.
        assert_eq!("\u{3000}1".trim(), "1");
        assert_eq!(trim_all("\u{3000}1"), "\u{3000}1");
        assert_eq!(trim_java_string("\u{3000}1"), "\u{3000}1");
    }

    #[test]
    fn trims_both_ends_and_leaves_interior() {
        assert_eq!(trim_all("\u{0}\u{1}\u{b}\u{7f} 1 2 \u{7f}\u{1f}"), "1 2");
        assert_eq!(trim_java_string("\u{0}\u{1}\u{b} 1 2 \u{1f}"), "1 2");
        // Interior bytes are never touched.
        assert_eq!(trim_all("1\u{0}2"), "1\u{0}2");
        assert_eq!(trim_java_string("1\u{0}2"), "1\u{0}2");
    }

    #[test]
    fn delete_byte_is_trimmed_only_by_trim_all() {
        assert_eq!(trim_all("\u{7f}123\u{7f}"), "123");
        assert_eq!(trim_java_string("\u{7f}123\u{7f}"), "\u{7f}123\u{7f}");
    }

    #[test]
    fn all_whitespace_input_trims_to_empty() {
        assert_eq!(trim_all(" \t\u{7f}"), "");
        assert_eq!(trim_java_string(" \t\n"), "");
        assert_eq!(trim_all(""), "");
        assert_eq!(trim_java_string(""), "");
    }

    #[test]
    fn multibyte_content_is_preserved() {
        // A trailing multi-byte char must not be sliced into.
        assert_eq!(trim_all(" \u{3000}\u{e9} "), "\u{3000}\u{e9}");
        assert_eq!(trim_java_string(" \u{3000}\u{e9} "), "\u{3000}\u{e9}");
    }
}
