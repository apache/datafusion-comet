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

"""Fast regression tests for the real Iceberg inventory and workflow-matrix guard."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "check_iceberg_shards", Path(__file__).with_name("check-iceberg-shards.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class IcebergShardManifestTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="comet-shard-manifest-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidates = [f"fixture/Test{index}.class"
                           for index in range(1, CHECK.SHARD_COUNT + 1)]
        for index in range(1, CHECK.SHARD_COUNT + 1):
            self.write(index)

    def write(self, index, attempt=1, **overrides):
        manifest = dict(task=":test", count=CHECK.SHARD_COUNT, shard=index, attempt=attempt,
                        candidates=[self.candidates[index - 1]],
                        unshardedCandidates=self.candidates)
        manifest.update(overrides)
        path = self.root / f"shard-{index}-attempt-{attempt}.json"
        path.write_text(json.dumps(manifest))
        return path

    def test_complete_inventory(self):
        CHECK.verify_manifests(self.root, ":test")

    def test_matrix_and_partition_count_share_one_definition(self):
        path = self.root / "github-output"
        CHECK.write_workflow_matrix(path)
        values = dict(line.split("=", 1) for line in path.read_text().splitlines())
        matrix = json.loads(values["matrix"])
        indices = matrix["shard"]
        self.assertEqual(indices, list(range(1, int(values["count"]) + 1)))
        self.assertEqual(len(indices), len(set(indices)))
        # A second dimension changes job-total but must not change shard count.
        matrix["environment"] = ["first", "second"]
        self.assertEqual(int(values["count"]), len(matrix["shard"]))

    def test_missing_shard_fails(self):
        (self.root / "shard-1-attempt-1.json").unlink()
        with self.assertRaisesRegex(ValueError, "Expected shard indices"):
            CHECK.verify_manifests(self.root, ":test")

    def test_unexpected_shard_fails(self):
        self.write(1, shard=CHECK.SHARD_COUNT + 1)
        with self.assertRaisesRegex(ValueError, "Expected shard indices"):
            CHECK.verify_manifests(self.root, ":test")

    def test_overlapping_shards_fail(self):
        self.write(2, candidates=self.candidates[:2])
        with self.assertRaisesRegex(ValueError, "multiple shards"):
            CHECK.verify_manifests(self.root, ":test")

    def test_missing_candidate_fails(self):
        self.write(1, candidates=[])
        with self.assertRaisesRegex(ValueError, "coverage mismatch"):
            CHECK.verify_manifests(self.root, ":test")

    def test_extra_candidate_fails(self):
        self.write(1, candidates=[self.candidates[0], "fixture/Unexpected.class"])
        with self.assertRaisesRegex(ValueError, "coverage mismatch"):
            CHECK.verify_manifests(self.root, ":test")

    def test_inconsistent_baselines_fail(self):
        self.write(1, unshardedCandidates=self.candidates[:1])
        with self.assertRaisesRegex(ValueError, "inventories disagree"):
            CHECK.verify_manifests(self.root, ":test")

    def test_wrong_count_or_task_fails(self):
        for overrides in ({"count": CHECK.SHARD_COUNT * 2}, {"task": ":otherTest"}):
            with self.subTest(overrides=overrides):
                self.write(1, **overrides)
                with self.assertRaisesRegex(ValueError, "mismatched task or shard count"):
                    CHECK.verify_manifests(self.root, ":test")

    def test_duplicate_candidate_in_one_manifest_fails(self):
        self.write(1, candidates=[self.candidates[0]] * 2)
        with self.assertRaisesRegex(ValueError, "duplicate class paths"):
            CHECK.verify_manifests(self.root, ":test")

    def test_duplicate_same_attempt_fails(self):
        contents = (self.root / "shard-1-attempt-1.json").read_text()
        (self.root / "duplicate.json").write_text(contents)
        with self.assertRaisesRegex(ValueError, "Duplicate inventory"):
            CHECK.verify_manifests(self.root, ":test")

    def test_failed_job_rerun_uses_latest_attempt_per_shard(self):
        # The other shards need not rerun when just shard 1 is retried.
        self.write(1, candidates=[])
        self.write(1, attempt=2)
        CHECK.verify_manifests(self.root, ":test")

    def test_newer_bad_inventory_cannot_be_hidden_by_old_attempt(self):
        self.write(1, attempt=2, candidates=[])
        with self.assertRaisesRegex(ValueError, "coverage mismatch"):
            CHECK.verify_manifests(self.root, ":test")

    def test_empty_inventory_directory_fails(self):
        with self.assertRaisesRegex(ValueError, "Expected shard indices"):
            CHECK.verify_manifests(self.root / "missing", ":test")

    def test_malformed_metadata_fails(self):
        for overrides in ({"attempt": 0}, {"shard": True}, {"count": "4"}):
            with self.subTest(overrides=overrides):
                path = self.write(1)
                manifest = json.loads(path.read_text())
                manifest.update(overrides)
                path.write_text(json.dumps(manifest))
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    CHECK.verify_manifests(self.root, ":test")


if __name__ == "__main__":
    unittest.main()
