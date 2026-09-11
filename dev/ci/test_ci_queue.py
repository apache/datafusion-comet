# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "dev/ci" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


routing = load("compute-changes")
PR_JOBS = {"build_linux", "build_macos", "benchmark", "spark_3_5", "spark_4_1", "iceberg_1_11"}
OTHER_VERSIONS = {"spark_3_4", "spark_4_0", "iceberg_1_8", "iceberg_1_9", "iceberg_1_10"}
QUEUE_JOBS = PR_JOBS | OTHER_VERSIONS


class QueuePolicyTest(unittest.TestCase):
    def event(self, name, action="", labels=(), label=""):
        return {"name": name, "action": action, "labels": list(labels),
                "label": label}

    def allowed(self, event):
        return {job for job in routing.POLICY if routing.event_allows(job, event)}

    def test_queue_and_push_run_full_coverage_without_reducing_prs(self):
        self.assertEqual(self.allowed(self.event("pull_request", "synchronize")), PR_JOBS)
        self.assertEqual(self.allowed(self.event("push")), QUEUE_JOBS | {"docs"})
        self.assertEqual(self.allowed(self.event("merge_group", "checks_requested")), QUEUE_JOBS)

    def test_unknown_event_and_queue_action_cannot_launch_jobs(self):
        for event in (self.event("schedule"), self.event("merge_group", "destroyed")):
            self.assertEqual(self.allowed(event), set())

    def test_label_opt_in_does_not_rerun_primary(self):
        for label, expected in (("run-spark-3.4-tests", {"spark_3_4"}),
                                ("run-spark-4.0-tests", {"spark_4_0"}),
                                ("run-iceberg-tests", {"iceberg_1_8", "iceberg_1_9",
                                                       "iceberg_1_10"})):
            with self.subTest(label=label):
                self.assertEqual(self.allowed(self.event("pull_request", "labeled", [label], label)), expected)
                self.assertEqual(self.allowed(self.event("pull_request", "synchronize", [label])), PR_JOBS | expected)
                self.assertEqual(self.allowed(self.event("pull_request", "labeled", [label, "dependencies"], "dependencies")), set())

    def test_docs_and_combined_queue_changes(self):
        event = self.event("merge_group", "checks_requested")
        self.assertFalse(any(routing.compute(["docs/source/user-guide/overview.md"], event).values()))
        # The queue diff can include different integration edits from two PRs.
        jobs = routing.compute(["dev/diffs/3.5.9.diff", "dev/diffs/iceberg/1.11.0.diff"], event)
        self.assertEqual({k for k, v in jobs.items() if v}, {
            "spark_3_5", "iceberg_1_8", "iceberg_1_9", "iceberg_1_10", "iceberg_1_11"})
        jobs = routing.compute(["native/core/src/lib.rs"], event)
        self.assertEqual({k for k, v in jobs.items() if v}, QUEUE_JOBS - {"benchmark"})

    def test_manual_run_exercises_all_coverage(self):
        event = self.event("workflow_dispatch")
        self.assertEqual(self.allowed(event), QUEUE_JOBS | {"docs"})


class RequiredGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        cls.blocks = dict(re.findall(
            r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)",
            cls.workflow.split("\njobs:\n", 1)[1], re.M | re.S))
        cls.required = cls.blocks["ci-required"]
        # Exercise the actual shell step, not a separate copy of its predicate.
        cls.script = textwrap.dedent(cls.required.split("        run: |\n", 1)[1])

    def run_gate(self, results):
        needs = {job: {"result": result} for job, result in results.items()}
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", self.script],
            env={**os.environ, "NEEDS": json.dumps(needs)},
            capture_output=True, text=True).returncode

    def test_success_and_skipped_jobs_pass_without_a_job_registry(self):
        self.assertEqual(self.run_gate({"preflight": "success", "changes": "success",
                                       "new-test-job": "success", "docs": "skipped"}), 0)
        self.assertEqual(self.run_gate({"optional-job": "skipped"}), 0)

    def test_failed_cancelled_or_missing_result_blocks(self):
        for job in ("preflight", "changes", "new-test-job"):
            for result in ("failure", "cancelled", None):
                with self.subTest(job=job, result=result):
                    results = {"preflight": "success", "changes": "success",
                               "new-test-job": "success", "docs": "skipped"}
                    results[job] = result
                    self.assertNotEqual(self.run_gate(results), 0)

    def test_workflow_wiring_covers_every_job(self):
        dependencies = set(re.findall(r"^      - ([\w-]+)$", self.required, re.M))
        self.assertEqual(dependencies, set(self.blocks) - {"ci-required"})
        self.assertIn("if: ${{ always() }}", self.required)
        self.assertIn("github.event.action == 'labeled'", self.required)
        self.assertIn("'CI Optional' || 'CI Required'", self.required)
        self.assertIn("  merge_group:\n    types: [checks_requested]", self.workflow)
        self.assertIn('"${QUEUE_BASE_SHA:?}".."${QUEUE_HEAD_SHA:?}"', self.workflow)
        self.assertIn('          - "CI Required"', (ROOT / ".asf.yaml").read_text())


if __name__ == "__main__":
    unittest.main()
