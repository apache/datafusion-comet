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
COLLECTOR = runpy.run_path(str(ROOT / "dev/release/update-native-licenses.py"))
LICENSE = "META-INF/LICENSE"
NOTICE = "META-INF/NOTICE"
BUNDLE = "META-INF/licenses/native-dependencies.txt"


def native_platforms(entries):
    platforms = set()
    for name, header in entries:
        if not name.startswith("org/apache/comet/") or not name.endswith(("/libcomet.so", "/libcomet.dylib", "/comet.dll")):
            continue
        parts = name.split("/")
        platform, arch = parts[-3:-1]
        if platform == "linux" and arch in ("amd64", "x86_64", "aarch64", "aarch_64"):
            machine = 62 if arch in ("amd64", "x86_64") else 183
            valid = (header[:6] == b"\x7fELF\x02\x01" and len(header) >= 20
                     and int.from_bytes(header[18:20], "little") == machine)
        elif platform == "darwin" and arch in ("x86_64", "aarch64", "aarch_64"):
            machine = 0x01000007 if arch == "x86_64" else 0x0100000c
            valid = (header[:4] == b"\xcf\xfa\xed\xfe" and len(header) >= 8
                     and int.from_bytes(header[4:8], "little") == machine)
        else:
            raise ValueError(f"review unsupported Comet native payload: {name}")
        if not valid:
            raise ValueError(f"Comet native payload header disagrees with its platform/architecture: {name}")
        platforms.add(platform)
    return platforms


def notice_text(root, platforms):
    text = (root / "NOTICE.txt").read_text()
    if platforms:
        text += (root / "dev/licenses/native-NOTICE.txt").read_text()
    return text


def assemble(classes, output, base_license, root=ROOT):
    entries = []
    for path in classes.rglob("libcomet.*"):
        with path.open("rb") as source:
            entries.append((path.relative_to(classes).as_posix(), source.read(32)))
    platforms = native_platforms(entries)
    output.mkdir(parents=True, exist_ok=True)
    license = base_license.read_text()
    if platforms:
        license += (root / "dev/licenses/native-license-reference.txt").read_text()
    (output / "LICENSE").write_text(license)
    (output / "NOTICE").write_text(notice_text(root, platforms))
    (output / "native-dependencies.txt").write_text(COLLECTOR["packaged_bundle"](root, platforms))


def check_jar(path, root=ROOT):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        entries = []
        for name in names:
            if name.startswith("org/apache/comet/") and name.endswith(("/libcomet.so", "/libcomet.dylib", "/comet.dll")):
                with archive.open(name) as source:
                    entries.append((name, source.read(32)))
        platforms = native_platforms(entries)
        for name in (LICENSE, NOTICE, BUNDLE):
            if names.count(name) != 1:
                raise ValueError(f"{path}: expected exactly one {name}, found {names.count(name)}")
        license = archive.read(LICENSE).decode("utf-8")
        reference = (root / "dev/licenses/native-license-reference.txt").read_text().strip()
        if "Apache License" not in license or license.count(reference) != (1 if platforms else 0):
            raise ValueError(f"{path}: LICENSE must retain Apache-2.0 and reference the native licenses")
        expected_bundle = COLLECTOR["packaged_bundle"](root, platforms).encode()
        if archive.read(BUNDLE) != expected_bundle:
            raise ValueError(f"{path}: native licenses differ from the reviewed dependency bundle")
        if archive.read(NOTICE).decode("utf-8") != notice_text(root, platforms):
            raise ValueError(f"{path}: NOTICE must preserve Comet and native dependency notices")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jars", nargs="*", type=Path)
    parser.add_argument("--assemble", type=Path, metavar="CLASSES", help="assemble legal resources for the actual native payloads")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--base-license", type=Path)
    args = parser.parse_args()
    try:
        COLLECTOR["check"]()
        if args.assemble:
            if not args.output or not args.base_license or args.jars:
                parser.error("--assemble needs --output and --base-license, without jars")
            assemble(args.assemble, args.output, args.base_license)
        elif not args.jars:
            parser.error("provide jars or --assemble")
        for path in args.jars:
            check_jar(path)
            print(f"Native license materials present: {path.name}")
    except (ValueError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
