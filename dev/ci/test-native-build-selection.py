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

"""Exercise native-build and independent-check conditions from the real workflow.

The routing script owns event policy; the workflow only reads its boolean
outputs. Only that small expression subset is translated here, keeping these
tests dependency-free. actionlint separately checks GitHub's expression syntax
and workflow dependency graph.
"""

import importlib.util
import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONSUMERS = {
    "pr_build_linux": "build_linux",
    "spark_3_4": "spark_3_4",
    "spark_3_5": "spark_3_5",
    "spark_4_0": "spark_4_0",
    "spark_4_1": "spark_4_1",
    "iceberg_1_8": "iceberg_1_8",
    "iceberg_1_9": "iceberg_1_9",
    "iceberg_1_10": "iceberg_1_10",
    "iceberg_1_11": "iceberg_1_11",
}
DEFAULT = {"pr_build_linux", "spark_3_5", "spark_4_1", "iceberg_1_11"}
OPT_IN = ("run-spark-3.4-tests", "run-spark-4.0-tests", "run-iceberg-tests")
LINUX_CHECKS = "pr_build_linux_checks"


def conditions():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    expressions = {}
    for job in ["build_linux_native", LINUX_CHECKS, *CONSUMERS]:
        block = re.search(
            r"^  " + job + r":\n.*?(?=^  [a-z][a-z_0-9]*:\n|\Z)",
            workflow,
            re.M | re.S,
        ).group()
        match = re.search(
            r"^    if:[ \t]*(?:\|[ \t]*\n(?P<block>(?:^      .*\n?)+)|(?P<inline>[^\n]+))",
            block,
            re.M,
        )
        expression = " ".join((match.group("block") or match.group("inline")).split())
        # Event policy belongs in compute-changes.py. Reject a caller that
        # silently reintroduces an independent github.event condition.
        remaining = re.sub(
            r"needs\.changes\.outputs\.[a-z_0-9]+|==|'true'|\|\||[()\s]",
            "",
            expression,
        )
        if remaining:
            raise ValueError(f"{job}: unsupported routing condition: {expression}")
        expression = re.sub(
            r"needs\.changes\.outputs\.([a-z_0-9]+)",
            lambda match: f"changes[{match.group(1)!r}]",
            expression,
        )
        expression = expression.replace("||", " or ")
        expressions[job] = compile(expression, str(ROOT / ".github/workflows/ci.yml"), "eval")
    return expressions


class NativeBuildSelectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expressions = conditions()
        spec = importlib.util.spec_from_file_location(
            "compute_changes", ROOT / "dev/ci/compute-changes.py"
        )
        cls.filters = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.filters)

    def evaluate(self, flags):
        context = {"changes": {key: str(value).lower() for key, value in flags.items()}}
        return {
            name: eval(expression, {"__builtins__": {}}, context)
            for name, expression in self.expressions.items()
        }

    def cli_outputs(self, files, event):
        env = {
            **os.environ,
            "EVENT_NAME": event["name"],
            "EVENT_ACTION": event.get("action", ""),
            "LABEL_NAME": event.get("label", ""),
            "PR_LABELS": json.dumps(event.get("labels", [])),
        }
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as changed:
            changed.write("\n".join(files))
            changed.flush()
            result = subprocess.run(
                [sys.executable, str(ROOT / "dev/ci/compute-changes.py"), changed.name],
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
        flags = {}
        for line in result.stdout.splitlines():
            key, value = line.split("=", 1)
            self.assertIn(value, ("true", "false"))
            flags[key] = value == "true"
        self.assertEqual(set(flags), set(self.filters.FILTERS))
        return flags

    def assert_selected(self, flags, expected):
        selected = self.evaluate(flags)
        self.assertEqual({name for name in CONSUMERS if selected[name]}, expected)
        self.assertEqual(selected["build_linux_native"], bool(expected))
        self.assertEqual(selected[LINUX_CHECKS], "pr_build_linux" in expected)

    def assert_selection(
        self, files, expected, event="pull_request", action="synchronize", labels=(), label=""
    ):
        event = {"name": event, "action": action, "labels": labels, "label": label}
        # Manual dispatch bypasses path filtering in the script's CLI. Exercise
        # the actual entry point so an empty changed-file list still runs all jobs.
        if event["name"] == "workflow_dispatch":
            flags = self.cli_outputs(files, event)
        else:
            flags = self.filters.compute(files, event)
        self.assert_selected(flags, expected)

    def test_native_change_uses_default_pr_coverage(self):
        self.assert_selection(["native/core/src/lib.rs"], DEFAULT)

    def test_docs_and_benchmarks_do_not_build_native(self):
        for path in ("docs/source/user-guide/overview.md", "native/core/benches/parquet_read.rs"):
            with self.subTest(path=path):
                self.assert_selection([path], set())

    def test_spark_patch_does_not_require_linux_test_workflow(self):
        self.assert_selection(["dev/diffs/3.5.9.diff"], {"spark_3_5"})

    def test_legacy_patch_needs_opt_in(self):
        self.assert_selection(["dev/diffs/3.4.3.diff"], set())
        self.assert_selection(
            ["dev/diffs/3.4.3.diff"], {"spark_3_4"}, labels=("run-spark-3.4-tests",)
        )

    def test_unrelated_label_does_not_duplicate_existing_runs(self):
        self.assert_selection(
            ["native/core/src/lib.rs"], set(), action="labeled",
            labels=(*OPT_IN, "dependencies"), label="dependencies",
        )

    def test_new_spark_label_runs_only_selected_version(self):
        self.assert_selection(
            ["native/core/src/lib.rs"], {"spark_3_4"}, action="labeled",
            labels=OPT_IN, label="run-spark-3.4-tests",
        )

    def test_new_iceberg_label_runs_only_opt_in_versions(self):
        self.assert_selection(
            ["native/core/src/lib.rs"], {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10"},
            action="labeled", labels=OPT_IN, label="run-iceberg-tests",
        )

    def test_main_and_manual_runs_include_legacy_consumers(self):
        self.assert_selection(["native/core/src/lib.rs"], set(CONSUMERS), event="push")
        self.assert_selection([], set(CONSUMERS), event="workflow_dispatch")

    def test_producer_change_exercises_all_default_linux_consumers(self):
        self.assert_selection([".github/workflows/build_linux_native.yml"], DEFAULT)

    def test_independent_checks_change_selects_linux_checks_and_tests(self):
        self.assert_selection(
            [".github/workflows/pr_build_linux_checks.yml"], {"pr_build_linux"}
        )

    def test_label_event_cli_uses_only_the_new_gating_label(self):
        for label, expected in (
            ("run-iceberg-tests", {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10"}),
            ("dependencies", set()),
        ):
            with self.subTest(label=label):
                event = {
                    "name": "pull_request",
                    "action": "labeled",
                    "labels": [*OPT_IN, "dependencies"],
                    "label": label,
                }
                flags = self.cli_outputs(["native/core/src/lib.rs"], event)
                self.assert_selected(flags, expected)

    def test_producer_follows_policy_changes_without_workflow_edits(self):
        with mock.patch.dict(self.filters.POLICY, {"spark_3_4": ["pr", "push"]}):
            self.assert_selection(["dev/diffs/3.4.3.diff"], {"spark_3_4"})
        with mock.patch.dict(
            self.filters.POLICY, {"build_linux": ["push", "label:run-linux-tests"]}
        ):
            files = ["common/src/test/ExampleTest.java"]
            self.assert_selection(files, set())
            self.assert_selection(
                files, {"pr_build_linux"}, action="labeled",
                labels=("run-linux-tests",), label="run-linux-tests",
            )

    def test_producer_condition_matches_all_consumer_combinations(self):
        keys = list(CONSUMERS.values())
        label_sets = [
            tuple(label for label, selected in zip(OPT_IN, mask) if selected)
            for mask in itertools.product((False, True), repeat=len(OPT_IN))
        ]
        events = [{"name": "push"}, {"name": "workflow_dispatch"}, {"name": "schedule"}]
        for labels in label_sets:
            for action in ("opened", "synchronize", "reopened"):
                events.append({"name": "pull_request", "action": action, "labels": labels})
            for label in (*OPT_IN, "dependencies"):
                events.append({
                    "name": "pull_request", "action": "labeled",
                    "labels": labels, "label": label,
                })
        for mask in itertools.product((False, True), repeat=len(keys)):
            raw_flags = dict(zip(keys, mask))
            for event in events:
                # The flags consumed by ci.yml already include the policy.
                # Exhaust the raw path combinations through that same policy,
                # including the CLI's manual-dispatch override.
                flags = {
                    key: event["name"] == "workflow_dispatch" or (
                        raw_flags[key] and self.filters.event_allows(key, event)
                    )
                    for key in keys
                }
                selected = self.evaluate(flags)
                for consumer, key in CONSUMERS.items():
                    self.assertEqual(selected[consumer], flags[key], (raw_flags, event, consumer))
                self.assertEqual(
                    selected["build_linux_native"],
                    any(flags.values()),
                    (raw_flags, event),
                )
                self.assertEqual(selected[LINUX_CHECKS], flags["build_linux"], (raw_flags, event))


if __name__ == "__main__":
    unittest.main()
