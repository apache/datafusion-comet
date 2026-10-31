#!/usr/bin/env python3
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

"""Check the actual shaded artifact, including repeated/incremental packaging."""

import argparse
from pathlib import Path
import runpy
import zipfile

ROOT = Path(__file__).resolve().parents[2]
LICENSE = "META-INF/LICENSE"
NOTICE = "META-INF/NOTICE"
BUNDLE = "META-INF/licenses/native-dependencies.txt"


def check_jar(path, root=ROOT):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        for name in (LICENSE, NOTICE, BUNDLE):
            if names.count(name) != 1:
                raise ValueError(f"{path}: expected exactly one {name}, found {names.count(name)}")
        license = archive.read(LICENSE).decode("utf-8")
        reference = (root / "dev/licenses/native-license-reference.txt").read_text().strip()
        if "Apache License" not in license or license.count(reference) != 1:
            raise ValueError(f"{path}: LICENSE must retain Apache-2.0 and reference the native licenses")
        if archive.read(BUNDLE) != (root / "dev/licenses/native-dependencies.txt").read_bytes():
            raise ValueError(f"{path}: native licenses differ from the reviewed dependency bundle")
        expected = ((root / "NOTICE.txt").read_text()
                    + (root / "dev/licenses/native-NOTICE.txt").read_text())
        if archive.read(NOTICE).decode("utf-8") != expected:
            raise ValueError(f"{path}: NOTICE must preserve Comet and native dependency notices")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jars", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        runpy.run_path(str(ROOT / "dev/release/update-native-licenses.py"))["check"]()
        for path in args.jars:
            check_jar(path)
            print(f"Native license materials present: {path.name}")
    except (ValueError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
