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

/// Spark UTF8_LCASE uses ICU's context-independent lowercase mapping, except that
/// dotted I expands to two characters and final sigma maps to non-final sigma.
/// Pin the mapping to Spark's Unicode version, independently of the Rust compiler.
pub(super) fn lowercase(c: char, unicode_version: u32) -> (char, Option<char>) {
    if c == '\u{130}' {
        return ('i', Some('\u{307}'));
    }
    let cp = c as u32;
    // Unicode 17 adds these mappings to Unicode 16 (ICU 78 versus ICU 76/77).
    if unicode_version == 17 {
        match cp {
            0xa7ce | 0xa7d2 | 0xa7d4 => return (char::from_u32(cp + 1).unwrap(), None),
            0x16ea0..=0x16eb8 => return (char::from_u32(cp + 0x1b).unwrap(), None),
            _ => {}
        }
    }
    let index = LOWERCASE_RANGES.partition_point(|&(start, _, _, _)| start <= cp);
    let lower = match LOWERCASE_RANGES[..index].last() {
        Some(&(start, end, stride, delta)) if cp <= end && (cp - start).is_multiple_of(stride) => {
            char::from_u32((cp as i32 + delta) as u32).unwrap()
        }
        _ => c,
    };
    (lower, None)
}

// Regenerate with: python3 dev/generate-spark-lcase.py
// ICU UnicodeData sources and SHA-256 digests are pinned in that script.
// The Unicode mapping data is distributed under Unicode-3.0; see LICENSE.txt.
// Entries are (first, last, stride, lowercase delta); includes Spark's final sigma rule.
// BEGIN GENERATED UNICODE 16 LOWERCASE RANGES
const LOWERCASE_RANGES: &[(u32, u32, u32, i32)] = &[
    (0x41, 0x5a, 1, 32),
    (0xc0, 0xd6, 1, 32),
    (0xd8, 0xde, 1, 32),
    (0x100, 0x12e, 2, 1),
    (0x132, 0x136, 2, 1),
    (0x139, 0x147, 2, 1),
    (0x14a, 0x176, 2, 1),
    (0x178, 0x178, 1, -121),
    (0x179, 0x17d, 2, 1),
    (0x181, 0x181, 1, 210),
    (0x182, 0x184, 2, 1),
    (0x186, 0x186, 1, 206),
    (0x187, 0x187, 1, 1),
    (0x189, 0x18a, 1, 205),
    (0x18b, 0x18b, 1, 1),
    (0x18e, 0x18e, 1, 79),
    (0x18f, 0x18f, 1, 202),
    (0x190, 0x190, 1, 203),
    (0x191, 0x191, 1, 1),
    (0x193, 0x193, 1, 205),
    (0x194, 0x194, 1, 207),
    (0x196, 0x196, 1, 211),
    (0x197, 0x197, 1, 209),
    (0x198, 0x198, 1, 1),
    (0x19c, 0x19c, 1, 211),
    (0x19d, 0x19d, 1, 213),
    (0x19f, 0x19f, 1, 214),
    (0x1a0, 0x1a4, 2, 1),
    (0x1a6, 0x1a6, 1, 218),
    (0x1a7, 0x1a7, 1, 1),
    (0x1a9, 0x1a9, 1, 218),
    (0x1ac, 0x1ac, 1, 1),
    (0x1ae, 0x1ae, 1, 218),
    (0x1af, 0x1af, 1, 1),
    (0x1b1, 0x1b2, 1, 217),
    (0x1b3, 0x1b5, 2, 1),
    (0x1b7, 0x1b7, 1, 219),
    (0x1b8, 0x1b8, 1, 1),
    (0x1bc, 0x1bc, 1, 1),
    (0x1c4, 0x1c4, 1, 2),
    (0x1c5, 0x1c5, 1, 1),
    (0x1c7, 0x1c7, 1, 2),
    (0x1c8, 0x1c8, 1, 1),
    (0x1ca, 0x1ca, 1, 2),
    (0x1cb, 0x1db, 2, 1),
    (0x1de, 0x1ee, 2, 1),
    (0x1f1, 0x1f1, 1, 2),
    (0x1f2, 0x1f4, 2, 1),
    (0x1f6, 0x1f6, 1, -97),
    (0x1f7, 0x1f7, 1, -56),
    (0x1f8, 0x21e, 2, 1),
    (0x220, 0x220, 1, -130),
    (0x222, 0x232, 2, 1),
    (0x23a, 0x23a, 1, 10795),
    (0x23b, 0x23b, 1, 1),
    (0x23d, 0x23d, 1, -163),
    (0x23e, 0x23e, 1, 10792),
    (0x241, 0x241, 1, 1),
    (0x243, 0x243, 1, -195),
    (0x244, 0x244, 1, 69),
    (0x245, 0x245, 1, 71),
    (0x246, 0x24e, 2, 1),
    (0x370, 0x372, 2, 1),
    (0x376, 0x376, 1, 1),
    (0x37f, 0x37f, 1, 116),
    (0x386, 0x386, 1, 38),
    (0x388, 0x38a, 1, 37),
    (0x38c, 0x38c, 1, 64),
    (0x38e, 0x38f, 1, 63),
    (0x391, 0x3a1, 1, 32),
    (0x3a3, 0x3ab, 1, 32),
    (0x3c2, 0x3c2, 1, 1),
    (0x3cf, 0x3cf, 1, 8),
    (0x3d8, 0x3ee, 2, 1),
    (0x3f4, 0x3f4, 1, -60),
    (0x3f7, 0x3f7, 1, 1),
    (0x3f9, 0x3f9, 1, -7),
    (0x3fa, 0x3fa, 1, 1),
    (0x3fd, 0x3ff, 1, -130),
    (0x400, 0x40f, 1, 80),
    (0x410, 0x42f, 1, 32),
    (0x460, 0x480, 2, 1),
    (0x48a, 0x4be, 2, 1),
    (0x4c0, 0x4c0, 1, 15),
    (0x4c1, 0x4cd, 2, 1),
    (0x4d0, 0x52e, 2, 1),
    (0x531, 0x556, 1, 48),
    (0x10a0, 0x10c5, 1, 7264),
    (0x10c7, 0x10c7, 1, 7264),
    (0x10cd, 0x10cd, 1, 7264),
    (0x13a0, 0x13ef, 1, 38864),
    (0x13f0, 0x13f5, 1, 8),
    (0x1c89, 0x1c89, 1, 1),
    (0x1c90, 0x1cba, 1, -3008),
    (0x1cbd, 0x1cbf, 1, -3008),
    (0x1e00, 0x1e94, 2, 1),
    (0x1e9e, 0x1e9e, 1, -7615),
    (0x1ea0, 0x1efe, 2, 1),
    (0x1f08, 0x1f0f, 1, -8),
    (0x1f18, 0x1f1d, 1, -8),
    (0x1f28, 0x1f2f, 1, -8),
    (0x1f38, 0x1f3f, 1, -8),
    (0x1f48, 0x1f4d, 1, -8),
    (0x1f59, 0x1f5f, 2, -8),
    (0x1f68, 0x1f6f, 1, -8),
    (0x1f88, 0x1f8f, 1, -8),
    (0x1f98, 0x1f9f, 1, -8),
    (0x1fa8, 0x1faf, 1, -8),
    (0x1fb8, 0x1fb9, 1, -8),
    (0x1fba, 0x1fbb, 1, -74),
    (0x1fbc, 0x1fbc, 1, -9),
    (0x1fc8, 0x1fcb, 1, -86),
    (0x1fcc, 0x1fcc, 1, -9),
    (0x1fd8, 0x1fd9, 1, -8),
    (0x1fda, 0x1fdb, 1, -100),
    (0x1fe8, 0x1fe9, 1, -8),
    (0x1fea, 0x1feb, 1, -112),
    (0x1fec, 0x1fec, 1, -7),
    (0x1ff8, 0x1ff9, 1, -128),
    (0x1ffa, 0x1ffb, 1, -126),
    (0x1ffc, 0x1ffc, 1, -9),
    (0x2126, 0x2126, 1, -7517),
    (0x212a, 0x212a, 1, -8383),
    (0x212b, 0x212b, 1, -8262),
    (0x2132, 0x2132, 1, 28),
    (0x2160, 0x216f, 1, 16),
    (0x2183, 0x2183, 1, 1),
    (0x24b6, 0x24cf, 1, 26),
    (0x2c00, 0x2c2f, 1, 48),
    (0x2c60, 0x2c60, 1, 1),
    (0x2c62, 0x2c62, 1, -10743),
    (0x2c63, 0x2c63, 1, -3814),
    (0x2c64, 0x2c64, 1, -10727),
    (0x2c67, 0x2c6b, 2, 1),
    (0x2c6d, 0x2c6d, 1, -10780),
    (0x2c6e, 0x2c6e, 1, -10749),
    (0x2c6f, 0x2c6f, 1, -10783),
    (0x2c70, 0x2c70, 1, -10782),
    (0x2c72, 0x2c72, 1, 1),
    (0x2c75, 0x2c75, 1, 1),
    (0x2c7e, 0x2c7f, 1, -10815),
    (0x2c80, 0x2ce2, 2, 1),
    (0x2ceb, 0x2ced, 2, 1),
    (0x2cf2, 0x2cf2, 1, 1),
    (0xa640, 0xa66c, 2, 1),
    (0xa680, 0xa69a, 2, 1),
    (0xa722, 0xa72e, 2, 1),
    (0xa732, 0xa76e, 2, 1),
    (0xa779, 0xa77b, 2, 1),
    (0xa77d, 0xa77d, 1, -35332),
    (0xa77e, 0xa786, 2, 1),
    (0xa78b, 0xa78b, 1, 1),
    (0xa78d, 0xa78d, 1, -42280),
    (0xa790, 0xa792, 2, 1),
    (0xa796, 0xa7a8, 2, 1),
    (0xa7aa, 0xa7aa, 1, -42308),
    (0xa7ab, 0xa7ab, 1, -42319),
    (0xa7ac, 0xa7ac, 1, -42315),
    (0xa7ad, 0xa7ad, 1, -42305),
    (0xa7ae, 0xa7ae, 1, -42308),
    (0xa7b0, 0xa7b0, 1, -42258),
    (0xa7b1, 0xa7b1, 1, -42282),
    (0xa7b2, 0xa7b2, 1, -42261),
    (0xa7b3, 0xa7b3, 1, 928),
    (0xa7b4, 0xa7c2, 2, 1),
    (0xa7c4, 0xa7c4, 1, -48),
    (0xa7c5, 0xa7c5, 1, -42307),
    (0xa7c6, 0xa7c6, 1, -35384),
    (0xa7c7, 0xa7c9, 2, 1),
    (0xa7cb, 0xa7cb, 1, -42343),
    (0xa7cc, 0xa7cc, 1, 1),
    (0xa7d0, 0xa7d0, 1, 1),
    (0xa7d6, 0xa7da, 2, 1),
    (0xa7dc, 0xa7dc, 1, -42561),
    (0xa7f5, 0xa7f5, 1, 1),
    (0xff21, 0xff3a, 1, 32),
    (0x10400, 0x10427, 1, 40),
    (0x104b0, 0x104d3, 1, 40),
    (0x10570, 0x1057a, 1, 39),
    (0x1057c, 0x1058a, 1, 39),
    (0x1058c, 0x10592, 1, 39),
    (0x10594, 0x10595, 1, 39),
    (0x10c80, 0x10cb2, 1, 64),
    (0x10d50, 0x10d65, 1, 32),
    (0x118a0, 0x118bf, 1, 32),
    (0x16e40, 0x16e5f, 1, 32),
    (0x1e900, 0x1e921, 1, 34),
];
// END GENERATED UNICODE 16 LOWERCASE RANGES
