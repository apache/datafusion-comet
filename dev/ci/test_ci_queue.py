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
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "dev/ci" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


routing = load("compute-changes")
gate = load("check-ci-result")
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
    def fixture(self, planned=()):
        outputs = {key: str(key in planned).lower() for key in gate.JOBS}
        needs = {"preflight": {"result": "success"},
                 "changes": {"result": "success", "outputs": outputs}}
        needs.update({job: {"result": "success" if key in planned else "skipped"}
                      for key, job in gate.JOBS.items()})
        return needs

    def test_docs_skip_and_full_queue_success(self):
        self.assertEqual(gate.failures(self.fixture()), [])
        self.assertEqual(gate.failures(self.fixture(QUEUE_JOBS)), [])

    def test_any_required_group_failure_cancellation_or_skip_blocks(self):
        for job in gate.JOBS.values():
            for status in ("failure", "cancelled", "skipped", None):
                with self.subTest(job=job, status=status):
                    needs = self.fixture(gate.JOBS)
                    needs[job]["result"] = status
                    self.assertTrue(gate.failures(needs))

    def test_preflight_and_planner_cannot_hide_behind_skipped_children(self):
        for job in ("preflight", "changes"):
            for status in ("failure", "cancelled", "skipped"):
                needs = self.fixture()
                needs[job]["result"] = status
                self.assertTrue(gate.failures(needs))

    def test_missing_plan_and_unknown_dependency_fail_closed(self):
        needs = self.fixture()
        del needs["changes"]["outputs"]["iceberg_1_11"]
        self.assertTrue(gate.failures(needs))
        needs = self.fixture()
        needs["unknown"] = {"result": "success"}
        self.assertTrue(gate.failures(needs))
        needs = self.fixture()
        del needs["spark_4_1"]
        self.assertTrue(gate.failures(needs))

    def test_unplanned_failure_is_not_ignored(self):
        needs = self.fixture()
        needs["spark_4_1"]["result"] = "failure"
        self.assertTrue(gate.failures(needs))

    def test_workflow_wiring_covers_every_caller(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        blocks = dict(re.findall(r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)", workflow.split("\njobs:\n", 1)[1], re.M | re.S))
        self.assertEqual(set(blocks), {"preflight", "changes", "ci-required", *gate.JOBS.values()})
        required = blocks["ci-required"]
        for job in {"preflight", "changes", *gate.JOBS.values()}:
            self.assertIn(f"      - {job}\n", required)
        for key, job in gate.JOBS.items():
            self.assertIn(f"if: needs.changes.outputs.{key} == 'true'", blocks[job])
        self.assertIn("if: ${{ always() }}", required)
        self.assertIn("github.event.action == 'labeled'", required)
        self.assertIn("'CI Optional' || 'CI Required'", required)
        self.assertIn("  merge_group:\n    types: [checks_requested]", workflow)
        self.assertIn('"${QUEUE_BASE_SHA:?}".."${QUEUE_HEAD_SHA:?}"', workflow)
        self.assertIn('          - "CI Required"', (ROOT / ".asf.yaml").read_text())
        self.assertEqual(set(gate.JOBS), set(routing.POLICY))


if __name__ == "__main__":
    unittest.main()
