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

"""Fast regression tests for the Iceberg write report summary."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "summarize_iceberg_writes", Path(__file__).with_name("summarize-iceberg-writes.py"))
SUMMARIZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARIZE)

ARTIFACT = "iceberg-spark-1.11.0-spark-4.1.3-scala-2.13-jdk17-shard-{}-attempt-{}"


def records(*writers):
    return "".join(
        json.dumps({"writer": w, "node": "AppendData", "reasons": [], "failed": False}) + "\n"
        for w in writers)


class SummarizeIcebergWritesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="comet-iceberg-writes-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def attempt(self, shard, attempt, *writers):
        """A shard attempt's artifact as the coverage job downloads it.

        Every attempt uploads its test reports. Only an attempt that recorded writes has a
        report file.
        """
        artifact = self.root / ARTIFACT.format(shard, attempt)
        reports = artifact / "build/test-results/test"
        reports.mkdir(parents=True)
        (reports / "TEST-org.example.TestFixture.xml").write_text("<testsuite/>")
        if writers:
            writes = artifact / "build/comet-iceberg-writes"
            writes.mkdir(parents=True)
            (writes / "iceberg-writes-fixture.jsonl").write_text(records(*writers))

    def load(self):
        writes, missing = SUMMARIZE.load([self.root])
        return sorted(w["writer"] for w in writes), missing

    def test_every_shard_counts_once(self):
        self.attempt(1, 1, "native")
        self.attempt(2, 1, "jvm", "spark")
        self.assertEqual(self.load(), (["jvm", "native", "spark"], []))

    def test_latest_attempt_replaces_an_earlier_one(self):
        self.attempt(1, 1, "native", "native")
        self.attempt(1, 2, "jvm")
        self.assertEqual(self.load(), (["jvm"], []))

    def test_retry_without_writes_is_reported_missing_instead_of_stale(self):
        self.attempt(1, 1, "native")
        self.attempt(1, 2)
        self.attempt(2, 1, "spark")
        self.assertEqual(self.load(), (["spark"], [(1, 2)]))
        summary = SUMMARIZE.summarize("fixture", *SUMMARIZE.load([self.root]))
        self.assertIn("Shard 1 recorded no Iceberg writes in its latest attempt (2)", summary)
        self.assertIn("| Comet native writer | 0 | 0.0% |", summary)
        self.assertIn("| Spark V2 write, not planned by Comet's split operator | 1 | 100.0% |",
                      summary)

    def test_root_that_is_itself_a_shard_artifact(self):
        self.attempt(1, 3)
        writes, missing = SUMMARIZE.load([self.root / ARTIFACT.format(1, 3)])
        self.assertEqual((writes, missing), ([], [(1, 3)]))

    def test_files_outside_shard_artifacts_always_count(self):
        # A shard job and dev/local-ci.sh summarize their own report directory directly.
        (self.root / "iceberg-writes-local.jsonl").write_text(records("native", "jvm"))
        self.assertEqual(self.load(), (["jvm", "native"], []))

    def test_no_writes_at_all(self):
        summary = SUMMARIZE.summarize("fixture", *SUMMARIZE.load([self.root / "missing"]))
        self.assertIn("No Iceberg writes were recorded", summary)


if __name__ == "__main__":
    unittest.main()
