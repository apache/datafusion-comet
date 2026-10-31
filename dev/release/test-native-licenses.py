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

from pathlib import Path
import runpy
import shutil
import tempfile
import unittest
import warnings
import zipfile

ROOT = Path(__file__).resolve().parents[2]
COLLECTOR = runpy.run_path(str(ROOT / "dev/release/update-native-licenses.py"))
CHECKER = runpy.run_path(str(ROOT / "dev/release/check-native-licenses.py"))


class NativeLicenseTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.jar = Path(self.directory.name) / "comet.jar"
        self.entries = {
            "org/apache/comet/linux/aarch64/libcomet.so": b"native packaging fixture",
            CHECKER["LICENSE"]: ((ROOT / "LICENSE.txt").read_bytes()
                                 + (ROOT / "dev/licenses/native-license-reference.txt").read_bytes()),
            CHECKER["NOTICE"]: ((ROOT / "NOTICE.txt").read_bytes()
                                + (ROOT / "dev/licenses/native-NOTICE.txt").read_bytes()),
            CHECKER["BUNDLE"]: (ROOT / "dev/licenses/native-dependencies.txt").read_bytes(),
        }

    def write_jar(self):
        with zipfile.ZipFile(self.jar, "w") as archive:
            for name, data in self.entries.items():
                archive.writestr(name, data)

    def test_generated_bundle_is_current(self):
        COLLECTOR["check"]()

    def test_complete_native_jar(self):
        self.write_jar()
        CHECKER["check_jar"](self.jar)

    def test_missing_native_licenses_are_rejected(self):
        del self.entries[CHECKER["BUNDLE"]]
        self.write_jar()
        with self.assertRaisesRegex(ValueError, "expected exactly one"):
            CHECKER["check_jar"](self.jar)

    def test_missing_license_reference_is_rejected(self):
        self.entries[CHECKER["LICENSE"]] = (ROOT / "LICENSE.txt").read_bytes()
        self.write_jar()
        with self.assertRaisesRegex(ValueError, "reference the native licenses"):
            CHECKER["check_jar"](self.jar)

    def test_truncated_bundle_is_rejected(self):
        self.entries[CHECKER["BUNDLE"]] = b"MIT\nBSD-3-Clause\n"
        self.write_jar()
        with self.assertRaisesRegex(ValueError, "reviewed dependency bundle"):
            CHECKER["check_jar"](self.jar)

    def test_repeated_license_reference_is_rejected(self):
        self.entries[CHECKER["LICENSE"]] += (ROOT / "dev/licenses/native-license-reference.txt").read_bytes()
        self.write_jar()
        with self.assertRaisesRegex(ValueError, "reference the native licenses"):
            CHECKER["check_jar"](self.jar)

    def test_lost_native_notices_are_rejected(self):
        self.entries[CHECKER["NOTICE"]] = (ROOT / "NOTICE.txt").read_bytes()
        self.write_jar()
        with self.assertRaisesRegex(ValueError, "preserve Comet and native dependency notices"):
            CHECKER["check_jar"](self.jar)

    def test_repeated_shading_cannot_duplicate_legal_entries(self):
        self.write_jar()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(self.jar, "a") as archive:
                archive.writestr(CHECKER["LICENSE"], self.entries[CHECKER["LICENSE"]])
        with self.assertRaisesRegex(ValueError, "found 2"):
            CHECKER["check_jar"](self.jar)

    def test_distinct_copyrights_with_same_spdx_id_survive(self):
        data = {"licenses": [
            {"id": "MIT", "text": "Copyright Alice\nPermission text", "source_path": "LICENSE", "used_by": []},
            {"id": "MIT", "text": "Copyright Bob\nPermission text", "source_path": "LICENSE", "used_by": []},
        ]}
        text = COLLECTOR["license_bundle"](data)
        self.assertIn("Copyright Alice", text)
        self.assertIn("Copyright Bob", text)

    def test_new_missing_upstream_permissive_license_requires_review(self):
        data = {"licenses": [{"id": "MIT", "text": "canonical MIT template", "source_path": None,
                              "used_by": [{"crate": {"name": "new-dependency", "version": "1.0.0"}}]}]}
        with self.assertRaisesRegex(ValueError, "review missing upstream MIT"):
            COLLECTOR["license_bundle"](data)

    def test_dependency_and_feature_changes_require_regeneration(self):
        root = Path(self.directory.name) / "source"
        paths = list(COLLECTOR["inputs"]()) + [
            "dev/licenses/native-licenses.json", "dev/licenses/native-dependencies.txt",
            "dev/licenses/native-NOTICE.txt",
        ]
        for path in paths:
            destination = root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / path, destination)
        COLLECTOR["check"](root)
        for path in ("native/Cargo.lock", "native/core/Cargo.toml"):
            destination = root / path
            original = destination.read_bytes()
            destination.write_bytes(original + b"\n")
            with self.assertRaisesRegex(ValueError, "inputs changed"):
                COLLECTOR["check"](root)
            destination.write_bytes(original)

    def test_confirmed_copyrights_are_present(self):
        text = self.entries[CHECKER["BUNDLE"]].decode()
        for value in ("lz4_flex 0.14.0", "Copyright (c) 2020 Pascal Seitz",
                      "For Zstandard software", "Copyright (c) Meta Platforms, Inc. and affiliates.",
                      "Redistributions in binary form", "AWS Libcrypto (AWS-LC)"):
            self.assertIn(value, text)


if __name__ == "__main__":
    unittest.main()
