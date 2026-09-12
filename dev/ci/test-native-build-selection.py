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

"""Exercise shared native-build selection through the routing script and CLI.

Consumer outputs already include path and event policy. The native producer
must be their union, including for manual dispatch. check-ci-config.py checks
the workflow's output wiring; actionlint validates its syntax and dependencies.
"""

import importlib.util
import itertools
import json
import os
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
DEFAULT = {"pr_build_linux", "spark_4_1", "iceberg_1_11"}
OPT_IN = (
    "run-spark-3.4-tests", "run-spark-3.5-tests", "run-spark-4.0-tests", "run-iceberg-tests"
)


class NativeBuildSelectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Load one routing module for this suite; import errors fail setup.

        Store it on the test class. Individual policy/filter patches restore
        this module's dictionaries when their context exits, including failure.
        """
        spec = importlib.util.spec_from_file_location(
            "compute_changes", ROOT / "dev/ci/compute-changes.py"
        )
        cls.filters = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.filters)

    def cli_outputs(self, files, event):
        """Return validated boolean outputs from one real CLI invocation.

        Pass repository-relative `files` through a temporary text file and the
        event fields through a child-only environment. Neither input nor the
        parent environment changes. The temporary file closes on success or
        failure; a failed process raises, and malformed or missing outputs fail
        assertions rather than being interpreted as a skipped native build.
        """
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
            self.assertNotIn(key, flags)
            flags[key] = value == "true"
        self.assertEqual(set(flags), set(self.filters.FILTERS) | {"build_linux_native"})
        return flags

    def assert_selected(self, flags, expected):
        """Assert consumer job IDs and producer selection without mutating inputs.

        `flags` is the complete output-to-bool mapping; `expected` contains CI
        consumer job IDs, whose output keys are pinned in CONSUMERS. Return
        None on success; any missing key or selection mismatch fails the test.
        """
        self.assertEqual({job for job, key in CONSUMERS.items() if flags[key]}, expected)
        self.assertEqual(flags["build_linux_native"], bool(expected))

    def assert_selection(
        self, files, expected, event="pull_request", action="synchronize", labels=(), label=""
    ):
        """Check compute() for changed paths, event fields, and expected job IDs.

        Build a fresh event mapping from the supplied values and assert the
        complete native selection. No caller input changes; return None or
        propagate the computation/assertion failure. CLI behavior is exercised
        separately so both entry points cover manual dispatch's empty input.
        """
        event = {"name": event, "action": action, "labels": labels, "label": label}
        flags = self.filters.compute(files, event)
        self.assert_selected(flags, expected)

    def test_native_change_uses_default_pr_coverage(self):
        self.assert_selection(["native/core/src/lib.rs"], DEFAULT)

    def test_docs_and_benchmarks_do_not_build_native(self):
        for path in ("docs/source/user-guide/overview.md", "native/core/benches/parquet_read.rs"):
            with self.subTest(path=path):
                self.assert_selection([path], set())

    def test_spark_patch_does_not_require_linux_test_workflow(self):
        """Assert a default-tier Spark patch selects native without Linux tests."""
        self.assert_selection(["dev/diffs/4.1.3.diff"], {"spark_4_1"})

    def test_legacy_patch_needs_opt_in(self):
        """Assert queue-tier Spark patches need their label on ordinary PR runs.

        Each repository-relative patch path is tested with and without its
        matching label. Inputs and routing tables stay unchanged; a mismatch
        in either the consumer or producer selection fails the assertion.
        """
        for version, job, label in (
            ("3.4.3", "spark_3_4", "run-spark-3.4-tests"),
            ("3.5.9", "spark_3_5", "run-spark-3.5-tests"),
            ("4.0.4", "spark_4_0", "run-spark-4.0-tests"),
        ):
            with self.subTest(version=version):
                files = [f"dev/diffs/{version}.diff"]
                self.assert_selection(files, set())
                self.assert_selection(files, {job}, labels=(label,))

    def test_unrelated_label_does_not_duplicate_existing_runs(self):
        self.assert_selection(
            ["native/core/src/lib.rs"], set(), action="labeled",
            labels=(*OPT_IN, "dependencies"), label="dependencies",
        )

    def test_new_spark_label_runs_only_selected_version(self):
        """Assert each new Spark label selects only its consumer and native build.

        Use a shared native source path with every opt-in label present, so
        the event's newly added label must narrow the selection. No fixtures
        are mutated; incorrect selection fails through assert_selection().
        """
        for job, label in (
            ("spark_3_4", "run-spark-3.4-tests"),
            ("spark_3_5", "run-spark-3.5-tests"),
            ("spark_4_0", "run-spark-4.0-tests"),
        ):
            with self.subTest(label=label):
                self.assert_selection(
                    ["native/core/src/lib.rs"], {job}, action="labeled",
                    labels=OPT_IN, label=label,
                )

    def test_nonconsumer_labels_do_not_build_native(self):
        """Assert macOS and benchmark label runs select no shared native build.

        Both real routes match the changed paths, and all consumer opt-ins are
        present. Check that the newly labeled route runs while every native
        consumer stays off. No input or routing configuration is changed.
        """
        for label, key in (
            ("run-macos-tests", "build_macos"),
            ("run-benchmark-check", "benchmark"),
        ):
            with self.subTest(label=label):
                flags = self.filters.compute(
                    ["native/core/src/lib.rs", "native/core/benches/parquet_read.rs"],
                    {"name": "pull_request", "action": "labeled",
                     "labels": (*OPT_IN, label), "label": label},
                )
                self.assertTrue(flags[key])
                self.assert_selected(flags, set())

    def test_new_iceberg_label_runs_only_opt_in_versions(self):
        self.assert_selection(
            ["native/core/src/lib.rs"], {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10"},
            action="labeled", labels=OPT_IN, label="run-iceberg-tests",
        )

    def test_merge_queue_runs_include_legacy_consumers(self):
        """Assert the queue selects all native consumers in compute() and CLI.

        A native source edit matches every consumer without opt-in labels.
        Check both entry points; the CLI's temporary inputs are cleaned up
        by cli_outputs(), and neither check changes the routing policy.
        """
        files = ["native/core/src/lib.rs"]
        self.assert_selection(files, set(CONSUMERS), event="merge_group")
        self.assert_selected(self.cli_outputs(files, {"name": "merge_group"}), set(CONSUMERS))

    def test_main_runs_only_linux_consumers_to_refresh_caches(self):
        """Assert push selects the Linux consumer and native build in both APIs.

        Main's cache refresh needs the shared producer, while queue-only test
        consumers stay off. The native source path and event are read-only;
        cli_outputs() owns and cleans up its temporary changed-files input.
        """
        files = ["native/core/src/lib.rs"]
        self.assert_selection(files, {"pr_build_linux"}, event="push")
        self.assert_selected(self.cli_outputs(files, {"name": "push"}), {"pr_build_linux"})

    def test_manual_runs_include_legacy_consumers_without_changed_files(self):
        """Assert empty-input dispatch selects every consumer in compute and CLI."""
        self.assert_selection([], set(CONSUMERS), event="workflow_dispatch")
        flags = self.cli_outputs([], {"name": "workflow_dispatch"})
        self.assert_selected(flags, set(CONSUMERS))
        self.assertTrue(all(flags.values()))

    def test_empty_changes_and_unsupported_events_skip_native(self):
        """Assert ordinary empty diffs and unsupported events select no consumers."""
        self.assert_selection([], set())
        self.assert_selection(["native/core/src/lib.rs"], set(), event="schedule")

    def test_nonconsumer_outputs_do_not_select_native(self):
        """Select each unrelated route alone and ensure it cannot start native CI.

        Temporarily give every filter a distinct synthetic path to separate
        macOS from its normally overlapping Linux inputs. Use each route's
        permitted event so the assertion checks an active unrelated job. The
        patch restores the real filters on exit, including assertion failure.
        """
        filters = {key: [key] for key in self.filters.FILTERS}
        with mock.patch.dict(self.filters.FILTERS, filters, clear=True):
            for key, event in (
                ("build_macos", "merge_group"), ("benchmark", "merge_group"), ("docs", "push")
            ):
                with self.subTest(key=key):
                    flags = self.filters.compute([key], {"name": event})
                    self.assertEqual({name for name, selected in flags.items() if selected}, {key})

    def test_cli_emits_native_output_for_selected_and_skipped_runs(self):
        """Assert real CLI output includes the producer on both true and false paths."""
        event = {"name": "pull_request", "action": "synchronize", "labels": []}
        for files, expected in ((["native/core/src/lib.rs"], DEFAULT), ([], set())):
            with self.subTest(files=files):
                self.assert_selected(self.cli_outputs(files, event), expected)

    def test_producer_change_exercises_all_default_linux_consumers(self):
        self.assert_selection([".github/workflows/build_linux_native.yml"], DEFAULT)

    def test_independent_checks_change_selects_linux_checks_and_tests(self):
        self.assert_selection(
            [".github/workflows/pr_build_linux_checks.yml"], {"pr_build_linux"}
        )

    def test_label_event_cli_uses_only_the_new_gating_label(self):
        """Assert the CLI derives native selection from the new label only.

        Exercise Spark 3.5's queue opt-in, Iceberg's grouped opt-in, and an
        unrelated label. cli_outputs() isolates and cleans up the child
        environment and temporary file; routing tables remain unchanged.
        """
        for label, expected in (
            ("run-spark-3.5-tests", {"spark_3_5"}),
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

    def test_producer_output_matches_all_consumer_combinations(self):
        """Exhaust all 512 consumer path masks across event and label combinations.

        Synthetic one-path filters exercise compute() without relying on real
        paths overlapping particular consumers. Unrelated routes also match,
        guarding against accidentally including them in the native union. Only
        FILTERS is patched, and it is restored on success or assertion failure;
        the actual event policy and native-output computation always execute.
        Include every subset of native opt-in labels and the merge queue,
        cache-refresh push, manual dispatch, and unsupported schedule events.
        """
        keys = list(CONSUMERS.values())
        label_sets = [
            tuple(label for label, selected in zip(OPT_IN, mask) if selected)
            for mask in itertools.product((False, True), repeat=len(OPT_IN))
        ]
        events = [
            {"name": "merge_group"}, {"name": "push"},
            {"name": "workflow_dispatch"}, {"name": "schedule"},
        ]
        for labels in label_sets:
            for action in ("opened", "synchronize", "reopened"):
                events.append({"name": "pull_request", "action": action, "labels": labels})
            for label in (*OPT_IN, "dependencies"):
                events.append({
                    "name": "pull_request", "action": "labeled",
                    "labels": labels, "label": label,
                })
        filters = {key: [key] for key in self.filters.FILTERS}
        unrelated = sorted(set(filters) - set(keys))
        with mock.patch.dict(self.filters.FILTERS, filters, clear=True):
            for mask in itertools.product((False, True), repeat=len(keys)):
                raw_flags = dict(zip(keys, mask))
                files = [key for key, selected in raw_flags.items() if selected] + unrelated
                for event in events:
                    flags = self.filters.compute(files, event)
                    expected = {
                        job for job, key in CONSUMERS.items()
                        if event["name"] == "workflow_dispatch" or (
                            raw_flags[key] and self.filters.event_allows(key, event)
                        )
                    }
                    self.assert_selected(flags, expected)


if __name__ == "__main__":
    unittest.main()
