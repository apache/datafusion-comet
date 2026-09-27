#!/usr/bin/env python3
##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

# Fails if any class in the given jars, shaded dependencies included, has a
# class file version newer than --max (55 is Java 11, 61 is Java 17). Entries
# under META-INF/versions/ are skipped, because the JVM only loads a
# multi-release entry on the Java version it is for.

import argparse
import struct
import sys
import zipfile


def versions_above(jar, limit):
    """Returns {class file version: first class seen} for versions above limit."""
    found = {}
    with zipfile.ZipFile(jar) as z:
        for name in z.namelist():
            if not name.endswith(".class") or name.startswith("META-INF/versions/"):
                continue
            with z.open(name) as f:
                major = struct.unpack(">H", f.read(8)[6:8])[0]
            if major > limit:
                found.setdefault(major, name)
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, required=True,
                        help="newest class file version allowed, e.g. 55 for Java 11")
    parser.add_argument("jars", nargs="+")
    args = parser.parse_args()
    failed = False
    for jar in args.jars:
        for major, name in sorted(versions_above(jar, args.max).items()):
            print(f"{jar}: {name} has class file version {major}, newer than {args.max}")
            failed = True
    if failed:
        sys.exit(1)
    print(f"Checked {len(args.jars)} jars: no class file version is newer than {args.max}")


if __name__ == "__main__":
    main()
