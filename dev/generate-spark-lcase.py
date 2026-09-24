#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regenerate the Unicode 16 lowercase table and verify the Unicode 17 additions.

Sources are UnicodeData.txt from ICU 76.1 and ICU 78.3, pinned by commit and SHA-256.
The Unicode mapping data is distributed under Unicode-3.0; see LICENSE.txt.
Only Python's standard library is required. An optional directory argument reads
existing UnicodeData-16.txt and UnicodeData-17.txt files instead of downloading.
"""

import hashlib
from pathlib import Path
import sys
from urllib.request import urlopen


SOURCES = {
    16: (
        "8eca245c7484ac6cc179e3e5f7c1ea7680810f39",
        "ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f",
    ),
    17: (
        "21d1eb0f306e1141c10931e914dfc038c06121da",
        "2e1efc1dcb59c575eedf5ccae60f95229f706ee6d031835247d843c11d96470c",
    ),
}


def read_mapping(version):
    commit, digest = SOURCES[version]
    if len(sys.argv) > 1:
        data = (Path(sys.argv[1]) / f"UnicodeData-{version}.txt").read_bytes()
    else:
        url = (
            f"https://raw.githubusercontent.com/unicode-org/icu/{commit}/"
            "icu4c/source/data/unidata/UnicodeData.txt"
        )
        with urlopen(url) as response:
            data = response.read()
    assert hashlib.sha256(data).hexdigest() == digest
    mapping = {}
    for line in data.decode().splitlines():
        fields = line.split(";")
        cp = int(fields[0], 16)
        if fields[13] and cp != 0x130:  # Dotted I expands in lowercase().
            mapping[cp] = int(fields[13], 16)
    mapping[0x3C2] = 0x3C3  # Spark maps final sigma to non-final sigma.
    return mapping


unicode16, unicode17 = read_mapping(16), read_mapping(17)
expected17 = dict(unicode16)
expected17.update({cp: cp + 1 for cp in (0xA7CE, 0xA7D2, 0xA7D4)})
expected17.update({cp: cp + 0x1B for cp in range(0x16EA0, 0x16EB9)})
assert unicode17 == expected17  # Keep lowercase()'s four overrides in sync.

pairs = sorted(unicode16.items())
rows = []
index = 0
while index < len(pairs):
    start, lower = pairs[index]
    delta = lower - start
    stride = 1
    if index + 1 < len(pairs):
        next_cp, next_lower = pairs[index + 1]
        if next_lower - next_cp == delta and next_cp - start == 2:
            stride = 2
    end = start
    index += 1
    while index < len(pairs) and pairs[index] == (end + stride, end + stride + delta):
        end += stride
        index += 1
    rows.append(f"    (0x{start:x}, 0x{end:x}, {stride}, {delta}),")

target = Path(__file__).resolve().parents[1] / (
    "native/spark-expr/src/array_funcs/array_extrema/unicode_lowercase.rs"
)
begin = "// BEGIN GENERATED UNICODE 16 LOWERCASE RANGES\n"
end = "// END GENERATED UNICODE 16 LOWERCASE RANGES"
prefix, generated = target.read_text().split(begin)
_, suffix = generated.split(end)
table = "const LOWERCASE_RANGES: &[(u32, u32, u32, i32)] = &[\n"
target.write_text(prefix + begin + table + "\n".join(rows) + "\n];\n" + end + suffix)
print(f"Generated {len(rows)} ranges in {target}")
