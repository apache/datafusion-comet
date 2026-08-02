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

//! Whitespace trimming for parsing from string.
//!
//! Spark's string casts do not all agree on what "whitespace" means. There are exactly two
//! regimes, and neither of them matches Rust's `str::trim` (which trims Unicode whitespace) or
//! `<[u8]>::trim_ascii` (which omits `0x0B`):
//!
//! | Regime               | Trimmed bytes            | Cast targets                                       |
//! |----------------------|--------------------------|----------------------------------------------------|
//! | [`trim_all`]         | `0x00`-`0x20` and `0x7F` | boolean, byte, short, int, long, date, timestamp \* |
//! | [`trim_java_string`] | `0x00`-`0x20`            | float, double, decimal                             |
//!
//! Neither regime trims non-ASCII whitespace (`str::trim` does, which is why it cannot be reused
//! here). The `String.trim` regime comes from the JDK calls Spark delegates to:
//! `Double.parseDouble` trims before parsing, and `Decimal.stringToJavaBigDecimal` does
//! `str.toString.trim`.
//!
//! \* `timestamp` and `timestamp_ntz` are listed for what Spark does; the Comet parsers for those
//! two targets have not been migrated to these helpers and still use `str::trim`, as do `to_time`
//! and `try_to_time` in `datetime_funcs::to_time`
//! (<https://github.com/apache/datafusion-comet/issues/5149>).

/// True for the bytes trimmed by `org.apache.spark.unsafe.types.UTF8String.trimAll`, i.e. the
/// bytes `b` for which `Character.isWhitespace(b) || Character.isISOControl(b)` holds, which
/// reduces to `b <= 0x20 || b == 0x7F`.
///
/// Spark widens a *signed* `byte` into the `int` overload, so bytes `0x80`-`0xFF` arrive negative
/// and are never trimmed.
#[inline]
const fn is_whitespace_or_iso_control(b: u8) -> bool {
    b <= 0x20 || b == 0x7F
}

/// True for the bytes trimmed by `java.lang.String.trim`, which drops any char `<= U+0020`.
///
/// In valid UTF-8 a byte `<= 0x20` is always a single-byte codepoint, since the lead and
/// continuation bytes of a multi-byte sequence are all `>= 0x80`. So testing bytes rather than
/// chars gives the same answer.
#[inline]
const fn is_java_trim_byte(b: u8) -> bool {
    b <= 0x20
}

/// Trims the `UTF8String.trimAll` byte set (`0x00`-`0x20` and `0x7F`) from both ends.
///
/// See the [module docs](self) for which cast targets use this regime and which use
/// [`trim_java_string`].
#[inline]
pub(crate) fn trim_all(s: &str) -> &str {
    let (start, end) = trim_all_range(s.as_bytes());
    &s[start..end]
}

/// [`trim_all`] over a byte slice, for parsers that already work on bytes.
#[inline]
pub(crate) fn trim_all_bytes(bytes: &[u8]) -> &[u8] {
    trim_bytes(bytes, is_whitespace_or_iso_control).0
}

/// The byte offsets [`trim_all`] would slice at, for parsers that need to keep a cursor into
/// the untrimmed input.
#[inline]
pub(crate) fn trim_all_range(bytes: &[u8]) -> (usize, usize) {
    let (trimmed, start) = trim_bytes(bytes, is_whitespace_or_iso_control);
    (start, start + trimmed.len())
}

/// Trims the `java.lang.String.trim` byte set (`0x00`-`0x20`, keeping `0x7F`) from both ends.
///
/// See the [module docs](self) for which cast targets use this regime and which use [`trim_all`].
#[inline]
pub(crate) fn trim_java_string(s: &str) -> &str {
    let (trimmed, start) = trim_bytes(s.as_bytes(), is_java_trim_byte);
    &s[start..start + trimmed.len()]
}

/// `bytes` with every leading and trailing byte matching `trimmed` removed, plus the number of
/// bytes removed from the front.
///
/// Written with slice patterns rather than indexing -- the same shape as `<[u8]>::trim_ascii` --
/// so the scan compiles to a leaf with no bounds checks. These run per row in the cast kernels,
/// and an indexed version measurably slows the integral casts down.
///
/// Slicing a `&str` on the returned offsets cannot panic as long as `trimmed` only accepts
/// ASCII bytes, since those never appear inside a multi-byte UTF-8 sequence.
#[inline]
fn trim_bytes(bytes: &[u8], trimmed: impl Fn(u8) -> bool) -> (&[u8], usize) {
    let mut start = 0;
    let mut rest = bytes;
    while let [first, tail @ ..] = rest {
        if !trimmed(*first) {
            break;
        }
        start += 1;
        rest = tail;
    }
    while let [head @ .., last] = rest {
        if !trimmed(*last) {
            break;
        }
        rest = head;
    }
    (rest, start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_sets_match_spark() {
        for b in 0..=u8::MAX {
            // Derived from Java's `Character.isWhitespace` / `isISOControl` rather than from
            // the implementation under test.
            let is_whitespace = matches!(b, 0x09..=0x0D | 0x1C..=0x1F | 0x20);
            let is_iso_control = matches!(b, 0x00..=0x1F | 0x7F);
            assert_eq!(
                is_whitespace_or_iso_control(b),
                is_whitespace || is_iso_control,
                "byte 0x{b:02X} classified incorrectly for the trimAll set"
            );
            assert_eq!(
                is_java_trim_byte(b),
                b <= 0x20,
                "byte 0x{b:02X} classified incorrectly for the String.trim set"
            );
        }
        // The two regimes differ in exactly one byte: DELETE.
        let differing: Vec<u8> = (0..=u8::MAX)
            .filter(|&b| is_whitespace_or_iso_control(b) != is_java_trim_byte(b))
            .collect();
        assert_eq!(differing, vec![0x7F]);
        // `<[u8]>::trim_ascii` omits the vertical tab, which is why it cannot be used here.
        assert!(is_whitespace_or_iso_control(0x0B) && !0x0Bu8.is_ascii_whitespace());
    }

    #[test]
    fn trims_both_ends_and_leaves_interior() {
        assert_eq!(trim_all("\u{0}\u{1}\u{b}\u{7f} 1 2 \u{7f}\u{1f}"), "1 2");
        assert_eq!(trim_java_string("\u{0}\u{1}\u{b} 1 2 \u{1f}"), "1 2");
        // Interior bytes are never touched.
        assert_eq!(trim_all("1\u{0}2"), "1\u{0}2");
        assert_eq!(trim_java_string("1\u{0}2"), "1\u{0}2");
        // DELETE is trimmed by one regime only.
        assert_eq!(trim_all("\u{7f}123\u{7f}"), "123");
        assert_eq!(trim_java_string("\u{7f}123\u{7f}"), "\u{7f}123\u{7f}");
        // All-whitespace and empty input trim to empty.
        assert_eq!(trim_all(" \t\u{7f}"), "");
        assert_eq!(trim_java_string(" \t\n"), "");
        assert_eq!(trim_all(""), "");
        assert_eq!(trim_java_string(""), "");
    }

    #[test]
    fn non_ascii_whitespace_is_preserved() {
        // `str::trim` would strip U+3000 here; Spark never does. A trailing multi-byte char
        // must also not be sliced into.
        assert_eq!("\u{3000}1".trim(), "1");
        assert_eq!(trim_all(" \u{3000}\u{e9} "), "\u{3000}\u{e9}");
        assert_eq!(trim_java_string(" \u{3000}\u{e9} "), "\u{3000}\u{e9}");
    }

    #[test]
    fn byte_and_range_helpers_agree_with_str_helper() {
        for input in ["", " ", "\u{7f}\u{b}x\u{0}", "1 2", "\u{3000}x\u{3000}"] {
            let bytes = input.as_bytes();
            let (start, end) = trim_all_range(bytes);
            assert_eq!(trim_all_bytes(bytes), &bytes[start..end], "{input:?}");
            assert_eq!(
                trim_all(input).as_bytes(),
                trim_all_bytes(bytes),
                "{input:?}"
            );
        }
    }
}
