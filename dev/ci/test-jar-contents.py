#!/usr/bin/env python3
#
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

"""Check source-only JAR entries against both artifact roles using real ZIP files."""

from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "dev/ensure-jars-have-correct-contents.sh"
LICENSE = (ROOT / "dev/licenses/LICENSE-binary.txt").read_bytes()


class JarContents(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.main = self.jar("main.jar")

    def jar(self, name, entry=None):
        path = self.root / name
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("org/apache/comet/linux/amd64/libcomet.so", b"")
            archive.writestr("META-INF/LICENSE", LICENSE)
            if entry:
                archive.writestr(entry, "// source\n")
        return str(path)

    def check(self, *args):
        return subprocess.run(
            ["bash", str(CHECKER), *args], capture_output=True, text=True)

    def rejected(self, args, entry):
        result = self.check(*args)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("unexpected contents", result.stdout)
        self.assertIn(entry, result.stdout)

    def check_source_entry(self, entry):
        artifact = self.jar("payload.jar", entry)
        self.rejected([artifact], entry)
        result = self.check(self.main, artifact)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # Permissions must not leak into the main list, even for the same path.
        self.rejected([self.main + ";" + artifact, artifact], entry)
        self.rejected([artifact, artifact], entry)

    def test_module_descriptor(self):
        self.check_source_entry("module-info.java")

    def test_plugins_source(self):
        self.check_source_entry("org/apache/spark/Plugins.scala")

    def test_row_partition_source(self):
        self.check_source_entry("org/apache/spark/shuffle/sort/RowPartition.scala")

    def test_unrelated_source_rejected_in_both_roles(self):
        entry = "Unexpected.java"
        artifact = self.jar("unexpected.jar", entry)
        self.rejected([artifact], entry)
        self.rejected([self.main, artifact], entry)


if __name__ == "__main__":
    unittest.main()
