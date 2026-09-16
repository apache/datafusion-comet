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

"""Exercise native cache validation and failure handling without running a library."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location(
    "native_library_cache", Path(__file__).with_name("native-library-cache.py"))
CACHE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CACHE)


class NativeLibraryCacheTest(unittest.TestCase):
    """Use isolated, temporary cache/build paths; test bytes are never executable."""

    def setUp(self):
        """Create a multi-chunk source fixture; unittest owns directory cleanup."""
        temporary = tempfile.TemporaryDirectory(prefix="comet-native-cache-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cache = self.root / "cache"
        self.source = self.root / "built.so"
        self.destination = self.root / "native" / "target" / "ci" / "libcomet.so"
        self.output = self.root / "github-output"
        self.key = "comet-native-v1-expected-input-fingerprint"
        self.contents = b"native-library-test-bytes\x00" * 100000
        self.source.write_bytes(self.contents)

    def prepare(self):
        """Populate this fixture's cache from its source, propagating failures."""
        CACHE.prepare(self.key, self.cache, self.source)

    def assert_miss(self, key=None):
        """Require a successful CLI miss to delete an existing stale destination."""
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        self.destination.write_bytes(b"stale build must not be accepted")
        self.output.write_text("existing=value\n", encoding="utf-8")
        with redirect_stdout(io.StringIO()) as stdout:
            result = CACHE.main([
                "restore", "--key", key or self.key, "--cache-dir", str(self.cache),
                "--library", str(self.destination), "--github-output", str(self.output),
            ])
        self.assertEqual(result, 0)
        self.assertEqual(stdout.getvalue(), "hit=false\n")
        self.assertEqual(self.output.read_text(), "existing=value\nhit=false\n")
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(self.destination.parent.iterdir()), [])

    def test_prepare_and_restore_exact_bytes(self):
        """The CLI preserves bytes, records their SHA256, and appends a hit output."""
        self.assertEqual(CACHE.main([
            "prepare", "--key", self.key, "--cache-dir", str(self.cache),
            "--library", str(self.source),
        ]), 0)
        manifest = json.loads((self.cache / "manifest.json").read_text())
        self.assertEqual(manifest, {
            "key": self.key, "sha256": hashlib.sha256(self.contents).hexdigest(),
        })
        self.assertEqual(sorted(path.name for path in self.cache.iterdir()),
                         ["libcomet.so", "manifest.json"])
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"stale")
        self.output.write_text("existing=value\n", encoding="utf-8")
        with redirect_stdout(io.StringIO()) as stdout:
            result = CACHE.main([
                "restore", "--key", self.key, "--cache-dir", str(self.cache),
                "--library", str(self.destination), "--github-output", str(self.output),
            ])
        self.assertEqual(result, 0)
        self.assertEqual(stdout.getvalue(), "hit=true\n")
        self.assertEqual(self.output.read_text(), "existing=value\nhit=true\n")
        self.assertEqual(self.destination.read_bytes(), self.contents)
        self.assertEqual(list(self.destination.parent.iterdir()), [self.destination])

    def test_wrong_key_is_miss(self):
        """A valid binary from different native inputs cannot be reused."""
        self.prepare()
        self.assert_miss("comet-native-v1-different-input-fingerprint")

    def test_missing_cache_is_miss(self):
        """A cold cache removes stale output and falls through to a fresh build."""
        self.assert_miss()

    def test_missing_entry_file_is_miss(self):
        """Either absent cache file invalidates an otherwise complete entry."""
        for name in ("manifest.json", "libcomet.so"):
            with self.subTest(name=name):
                self.prepare()
                (self.cache / name).unlink()
                self.assert_miss()

    def test_corrupt_manifest_is_miss(self):
        """Malformed JSON/UTF8, invalid shapes and missing fields are misses."""
        for contents in (b"{", b"\xff", b"null", b"[]", b"{}",
                         json.dumps({"key": self.key}).encode(),
                         json.dumps({"key": self.key, "sha256": 123}).encode()):
            with self.subTest(contents=contents):
                self.prepare()
                (self.cache / "manifest.json").write_bytes(contents)
                self.assert_miss()

    def test_corrupt_or_truncated_library_is_miss(self):
        """The checksum rejects changed or incomplete bytes before installation."""
        for contents in (b"changed", b"", self.contents[:-1]):
            with self.subTest(length=len(contents)):
                self.prepare()
                (self.cache / "libcomet.so").write_bytes(contents)
                self.assert_miss()

    def test_symlinked_cache_files_are_misses(self):
        """Even symlinks to matching bytes are excluded from the cache contract."""
        for name in ("manifest.json", "libcomet.so"):
            with self.subTest(name=name):
                self.prepare()
                cached = self.cache / name
                target = self.root / f"linked-{name}"
                cached.replace(target)
                cached.symlink_to(target)
                self.assert_miss()

    def test_non_regular_cache_files_are_misses(self):
        """Reject directories and FIFOs without blocking or accepting stale output."""
        for name in ("manifest.json", "libcomet.so"):
            for kind in ("directory", "fifo"):
                with self.subTest(name=name, kind=kind):
                    self.prepare()
                    cached = self.cache / name
                    cached.unlink()
                    if kind == "directory":
                        cached.mkdir()
                    else:
                        os.mkfifo(cached)
                    self.assert_miss()
                    if kind == "directory":
                        cached.rmdir()
                    else:
                        cached.unlink()

    def test_unreadable_cache_is_miss(self):
        """A failed cache read is a miss even under a privileged test account."""
        self.prepare()
        with patch.object(CACHE, "open_regular_file", side_effect=PermissionError("unreadable")):
            self.assert_miss()

    def test_stale_destination_symlink_is_removed(self):
        """A miss unlinks the old output without touching its symlink target."""
        self.destination.parent.mkdir(parents=True)
        self.destination.symlink_to(self.source)
        self.assertFalse(CACHE.restore(self.key, self.cache, self.destination))
        self.assertFalse(self.destination.is_symlink())
        self.assertEqual(self.source.read_bytes(), self.contents)

    def test_destination_write_failure_is_fatal(self):
        """A verified cache cannot mask destination failures or leave partial output."""
        self.prepare()
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"stale")
        with patch.object(Path, "replace", side_effect=OSError("disk failure")):
            with redirect_stderr(io.StringIO()) as stderr:
                result = CACHE.main([
                    "restore", "--key", self.key, "--cache-dir", str(self.cache),
                    "--library", str(self.destination), "--github-output", str(self.output),
                ])
        self.assertEqual(result, 1)
        self.assertIn("disk failure", stderr.getvalue())
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.destination.parent.iterdir()), [])

    def test_failed_prepare_invalidates_previous_manifest(self):
        """An interrupted refresh cannot retain a manifest advertising success."""
        self.prepare()
        self.source.unlink()
        with self.assertRaises(FileNotFoundError):
            self.prepare()
        self.assertFalse((self.cache / "manifest.json").exists())
        self.assert_miss()

    def test_read_failure_discards_temporary_copy(self):
        """A mid-read failure preserves an old copy and cleans its temporary file."""
        self.destination.parent.mkdir(parents=True)
        self.destination.write_bytes(b"old complete copy")
        source = Mock()
        source.read.side_effect = [b"partial", OSError("read failure")]
        with self.assertRaises(ValueError):
            CACHE.copy_library(source, self.destination)
        self.assertEqual(self.destination.read_bytes(), b"old complete copy")
        self.assertEqual(list(self.destination.parent.iterdir()), [self.destination])


if __name__ == "__main__":
    unittest.main()
