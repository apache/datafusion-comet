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

"""Regression tests for shared artifacts using mutations of the real workflows."""

import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "check_ci_config", Path(__file__).with_name("check-ci-config.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
WORKFLOWS = Path(__file__).resolve().parents[2] / ".github/workflows"

# Pin every direct Maven caller affected by splitting the Linux workflow.
MAVEN_JOBS = {
    "pr_build_linux_checks.yml": (
        "lint-java", "build-spark-4-1", "celeborn-reflection-compatibility"),
    "pr_build_linux.yml": ("verify-benchmark-results-tpch", "verify-benchmark-results-tpcds"),
}


class SharedNativeArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="comet-ci-config-test-")
        self.addCleanup(self.temp.cleanup)
        self.workflows = Path(self.temp.name) / "workflows"
        shutil.copytree(WORKFLOWS, self.workflows)

    def replace(self, filename, old, new):
        path = self.workflows / filename
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text, f"Fixture changed: {filename}")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def assert_rejected(self, expected):
        failures = CHECK.artifact_failures(self.workflows)
        self.assertTrue(any(expected in failure for failure in failures), failures)

    def test_real_workflows_have_valid_shared_artifacts(self):
        self.assertEqual(CHECK.artifact_failures(self.workflows), [])

    def test_native_gates_cannot_bypass_selected_outputs(self):
        """Reject an inverted gate for each producer/consumer in a temporary copy.

        Each mutation starts from the original workflow and must report the
        changed job, including each side of the Spark 4.1 core/Hive OR. The
        source tree is untouched and the fixture is restored on success, or
        removed by tear-down if an assertion fails.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        selections = {CHECK.SHARED_NATIVE_JOB: (CHECK.SHARED_NATIVE_JOB,),
                      **CHECK.load_filters().NATIVE_CONSUMERS}
        for job_id, routes in selections.items():
            for output in routes:
                with self.subTest(job=job_id, output=output):
                    start = original.index(f"\n  {job_id}:\n")
                    expression = f"needs.changes.outputs.{output} == 'true'"
                    self.assertIn(expression, original[start:])
                    body = original[start:].replace(
                        expression, f"needs.changes.outputs.{output} == 'false'", 1)
                    path.write_text(original[:start] + body, encoding="utf-8")
                    self.assert_rejected(f"{job_id} must select exactly")
        path.write_text(original, encoding="utf-8")

    def test_native_outputs_must_be_exported_without_remapping(self):
        """Reject missing and remapped output exports in temporary workflows.

        Every producer/consumer output, including Hive's second route into
        Spark 4.1, is checked independently. This covers Python selecting work
        that ci.yml never starts. Only the temporary fixture is mutated; it is
        restored on success and removed by tear-down on any assertion failure.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        outputs = {CHECK.SHARED_NATIVE_JOB} | {
            output for routes in CHECK.load_filters().NATIVE_CONSUMERS.values() for output in routes}
        for output in sorted(outputs):
            expression = f"${{{{ steps.compute.outputs.{output} }}}}"
            self.assertIn(expression, original)
            for replacement in ("", "${{ steps.compute.outputs.docs }}"):
                with self.subTest(output=output, replacement=replacement):
                    path.write_text(original.replace(expression, replacement, 1), encoding="utf-8")
                    self.assert_rejected(f"changes must export steps.compute.outputs.{output}")
        path.write_text(original, encoding="utf-8")

    def test_hive_or_cannot_omit_or_add_selection_routes(self):
        """Reject narrowed or broadened Spark 4.1 gates in temporary ci.yml.

        Each mutation starts with the real core/Hive OR and tests missing
        branches, AND, a wrong Hive output, and an unrelated extra branch.
        No source files change; the fixture is restored on success and cleaned
        up by tear-down after either success or an assertion failure.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        core = "needs.changes.outputs.spark_4_1 == 'true'"
        hive = "needs.changes.outputs.spark_4_1_hive == 'true'"
        gate = f"{core} || {hive}"
        self.assertIn(gate, original)
        for replacement in (core, hive, f"{core} && {hive}",
                            f"{core} || needs.changes.outputs.docs == 'true'",
                            f"{gate} || needs.changes.outputs.docs == 'true'"):
            with self.subTest(gate=replacement):
                path.write_text(original.replace(gate, replacement, 1), encoding="utf-8")
                self.assert_rejected("spark_4_1 must select exactly")
        path.write_text(original, encoding="utf-8")

    def test_hive_route_must_remain_in_native_selector(self):
        """Reject omitting Hive from Python while ci.yml still selects it.

        Patch only the freshly imported selector returned to the checker;
        temporary and repository workflows remain unchanged. The mock scope
        restores load_filters even if the expected diagnostic is not raised.
        """
        selector = CHECK.load_filters()
        selector.NATIVE_CONSUMERS = {**selector.NATIVE_CONSUMERS, "spark_4_1": ("spark_4_1",)}
        with mock.patch.object(CHECK, "load_filters", return_value=selector):
            self.assert_rejected("spark_4_1 must select exactly")

    def test_native_consumer_calls_must_match_selector_ids(self):
        """Reject missing, renamed, or unrelated workflow calls for Spark 4.1.

        Keep the real Hive OR intact while mutating its caller in temporary
        ci.yml. The checker must identify the caller/selector mismatch rather
        than exempt the combined gate. Only the fixture is mutated; it is
        restored on success and removed by tear-down after assertion failures.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        body = CHECK.block_mapping(CHECK.block_mapping(original, 0)["jobs"][1], 2)["spark_4_1"][1]
        mutations = (
            original.replace(f"  spark_4_1:{body}", "", 1),
            original.replace("  spark_4_1:\n", "  spark_4_1_renamed:\n", 1),
            original.replace(body, body.replace("spark_sql_test_reusable.yml", "unrelated.yml"), 1),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=index):
                self.assertNotEqual(original, mutation)
                path.write_text(mutation, encoding="utf-8")
                self.assert_rejected("native consumer calls must match NATIVE_CONSUMERS")
        path.write_text(original, encoding="utf-8")

    def test_hive_gate_does_not_allow_missing_native_producer_call(self):
        """Reject a removed producer call even with valid core/Hive routing.

        Remove only the producer job from temporary ci.yml and require a
        producer diagnostic. The repository remains unchanged; tear-down
        releases the fixture directory even if the assertion fails.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        body = CHECK.block_mapping(CHECK.block_mapping(original, 0)["jobs"][1], 2)[
            CHECK.SHARED_NATIVE_JOB][1]
        declaration = f"  {CHECK.SHARED_NATIVE_JOB}:{body}"
        self.assertIn(declaration, original)
        path.write_text(original.replace(declaration, "", 1), encoding="utf-8")
        self.assert_rejected("expected exactly one build_linux_native call")

    def test_new_native_consumer_must_join_selector(self):
        """Reject an extra consumer absent from Python's producer union.

        Append a caller to the temporary workflow only. The fixture directory
        is removed by tear-down; no repository file is changed.
        """
        path = self.workflows / "ci.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n  spark_future:\n    needs: [changes, build_linux_native]\n"
                         "    uses: ./.github/workflows/spark_sql_test_reusable.yml\n"
                         "    with:\n      native-library-artifact: native-lib-linux\n")
        self.assert_rejected("native consumer calls must match NATIVE_CONSUMERS")

    def test_missing_shared_producer_is_rejected(self):
        (self.workflows / CHECK.SHARED_NATIVE_WORKFLOW).unlink()
        self.assert_rejected("shared native producer is missing")

    def test_second_producer_call_is_rejected(self):
        path = self.workflows / "ci.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n  duplicate_native:\n    needs: changes\n"
                         "    uses: ./.github/workflows/build_linux_native.yml\n")
        self.assert_rejected("expected exactly one")

    def test_another_workflow_cannot_publish_the_shared_name(self):
        shutil.copyfile(self.workflows / CHECK.SHARED_NATIVE_WORKFLOW,
                        self.workflows / "duplicate_native.yml")
        self.assert_rejected("only build_linux_native.yml may upload")

    def test_producer_matrix_is_rejected(self):
        self.replace(CHECK.SHARED_NATIVE_WORKFLOW, "    runs-on:",
                     "    strategy:\n      matrix:\n        duplicate: [1, 2]\n    runs-on:")
        self.assert_rejected("shared native producer must not use a matrix")

    def test_producer_artifact_rename_is_rejected(self):
        self.replace(CHECK.SHARED_NATIVE_WORKFLOW, "name: native-lib-linux",
                     "name: native-lib-renamed")
        self.assert_rejected("uploading 'native-lib-linux' once")

    def test_missing_dependency_in_each_consumer_call_is_rejected(self):
        """Reject each caller dropping its producer dependency independently.

        Caller IDs come from the selector mapping, not its output keys: Hive
        shares the spark_4_1 caller. Each mutation starts from real ci.yml in
        the temporary directory, restored on success and cleaned on failure.
        """
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        job_ids = CHECK.load_filters().NATIVE_CONSUMERS
        for job_id in sorted(job_ids):
            with self.subTest(job=job_id):
                start = original.index(f"\n  {job_id}:\n")
                before, body = original[:start], original[start:]
                self.assertIn("needs: [changes, build_linux_native]", body)
                body = body.replace("needs: [changes, build_linux_native]", "needs: changes", 1)
                path.write_text(before + body, encoding="utf-8")
                self.assert_rejected(f"{job_id} must need changes and build_linux_native")
        path.write_text(original, encoding="utf-8")

    def test_wrong_caller_artifact_is_rejected(self):
        self.replace("ci.yml", "native-library-artifact: native-lib-linux",
                     "native-library-artifact: native-lib-wrong")
        self.assert_rejected("must pass native-library-artifact: native-lib-linux")

    def test_missing_caller_artifact_is_rejected(self):
        self.replace("ci.yml", "      native-library-artifact: native-lib-linux\n", "")
        self.assert_rejected("must pass native-library-artifact: native-lib-linux")

    def test_consumer_input_must_be_required(self):
        self.replace("pr_build_linux.yml", "required: true", "required: false")
        self.assert_rejected("native-library-artifact must be a required string input")

    def test_consumer_input_must_be_string(self):
        self.replace("pr_build_linux.yml", "type: string", "type: boolean")
        self.assert_rejected("native-library-artifact must be a required string input")

    def test_literal_native_download_is_rejected(self):
        self.replace("pr_build_linux.yml", "name: ${{ inputs.native-library-artifact }}",
                     "name: native-lib-linux")
        self.assert_rejected("native downloads must use")

    def test_jvm_artifact_cannot_replace_a_native_download(self):
        path = self.workflows / "spark_sql_test_reusable.yml"
        uploads, _ = CHECK.artifact_names(path)
        jvm_name = next(name for name in uploads if name.startswith("jvm-compiled-spark-"))
        self.replace(path.name, "name: ${{ inputs.native-library-artifact }}", f"name: {jvm_name}")
        self.assert_rejected("native downloads must use")

    def test_unrelated_input_does_not_bypass_producer_check(self):
        self.replace("pr_build_linux.yml", "name: ${{ inputs.native-library-artifact }}",
                     "name: ${{ inputs.unrelated-artifact }}")
        self.assert_rejected("artifact '${{ inputs.unrelated-artifact }}' is downloaded but never uploaded")

    def test_native_build_in_consumer_is_rejected(self):
        path = self.workflows / "iceberg_spark_test_reusable.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n      - run: |\n          cargo build --profile ci\n")
        self.assert_rejected("consume the shared native library instead of building it")

    def test_jvm_artifact_rename_still_fails(self):
        self.replace("spark_sql_test_reusable.yml", "name: jvm-compiled-spark-",
                     "name: renamed-jvm-compiled-spark-")
        self.assert_rejected("is downloaded but never uploaded in the same workflow")

    def test_unqualified_jvm_upload_still_fails(self):
        path = self.workflows / "spark_sql_test_reusable.yml"
        text = path.read_text(encoding="utf-8")
        uploads, _ = CHECK.artifact_names(path)
        name = next(name for name in uploads if name.startswith("jvm-compiled-spark-"))
        path.write_text(text.replace(f"name: {name}", "name: jvm-compiled-spark"), encoding="utf-8")
        self.assert_rejected("qualify the name with an input")


    def test_independent_checks_cannot_wait_for_native(self):
        path = self.workflows / "ci.yml"
        text = path.read_text(encoding="utf-8")
        start = text.index("\n  pr_build_linux_checks:\n")
        before, body = text[:start], text[start:]
        self.assertIn("needs: changes", body)
        path.write_text(before + body.replace("needs: changes",
                        "needs: [changes, build_linux_native]", 1), encoding="utf-8")
        self.assert_rejected("pr_build_linux_checks must need only changes")

    def test_missing_independent_checks_call_is_rejected(self):
        self.replace("ci.yml", "uses: ./.github/workflows/pr_build_linux_checks.yml",
                     "uses: ./.github/workflows/pr_build_linux.yml")
        self.assert_rejected("pr_build_linux_checks must call")

    def test_missing_independent_checks_workflow_is_rejected(self):
        (self.workflows / CHECK.LINUX_CHECKS_WORKFLOW).unlink()
        self.assert_rejected("independent Linux checks workflow is missing")

    def test_independent_checks_keep_linux_selection(self):
        path = self.workflows / "ci.yml"
        text = path.read_text(encoding="utf-8")
        start = text.index("\n  pr_build_linux_checks:\n")
        before, body = text[:start], text[start:]
        body = body.replace("needs.changes.outputs.build_linux == 'true'",
                            "needs.changes.outputs.build_linux == 'false'", 1)
        path.write_text(before + body, encoding="utf-8")
        self.assert_rejected("pr_build_linux_checks must use the Linux test selection condition")

    def test_each_independent_job_must_remain_available(self):
        path = self.workflows / CHECK.LINUX_CHECKS_WORKFLOW
        original = path.read_text(encoding="utf-8")
        for job_id in sorted(CHECK.INDEPENDENT_LINUX_JOBS):
            with self.subTest(job=job_id):
                self.assertIn(f"\n  {job_id}:\n", original)
                path.write_text(original.replace(f"\n  {job_id}:\n",
                                f"\n  missing-{job_id}:\n", 1), encoding="utf-8")
                self.assert_rejected(f"independent jobs are missing: {job_id}")
        path.write_text(original, encoding="utf-8")

    def test_independent_job_cannot_return_to_native_consumer(self):
        path = self.workflows / "pr_build_linux.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n  linux-test-rust:\n    runs-on: ubuntu-24.04\n"
                         "    steps:\n      - run: true\n")
        self.assert_rejected("independent jobs must stay in pr_build_linux_checks.yml")

    def test_independent_checks_cannot_require_native_artifact_input(self):
        self.replace(CHECK.LINUX_CHECKS_WORKFLOW, "  workflow_call:\n",
                     "  workflow_call:\n    inputs:\n      native-library-artifact:\n"
                     "        required: true\n        type: string\n")
        self.assert_rejected("independent Linux checks must not consume the shared native artifact")

    def test_independent_checks_cannot_download_native_artifact(self):
        """Reject direct and retried downloads of native artifacts by independent checks.

        Each action is appended to a fresh copy of the temporary workflow. The
        original fixture is restored after successful assertions and its whole
        temporary directory is cleaned up even if a check fails.
        """
        path = self.workflows / CHECK.LINUX_CHECKS_WORKFLOW
        original = path.read_text(encoding="utf-8")
        for action in ("actions/download-artifact@v8", "./.github/actions/download-artifact-retry"):
            with self.subTest(action=action):
                path.write_text(original + f"\n      - uses: {action}\n"
                                "        with:\n          name: native-lib-linux\n"
                                "          path: native/target/release/\n", encoding="utf-8")
                self.assert_rejected("independent Linux checks must not consume the shared native artifact")
        path.write_text(original, encoding="utf-8")

    def test_every_linux_maven_job_keeps_earlier_reliable_bootstrap(self):
        """Reject four lost-bootstrap cases for all five direct Maven callers.

        For each explicitly named job, independently remove the bootstrap,
        move it after its Maven commands, make it conditional, or allow its
        errors. This exercises inline, multiline, and env-prefixed Maven runs.
        Only temporary workflow copies are mutated, restored between jobs on
        success and removed by tear-down after either success or failure.
        """
        bootstrap = f"      - name: Bootstrap Maven\n        uses: {CHECK.MAVEN_BOOTSTRAP_ACTION}\n"
        for filename, job_ids in MAVEN_JOBS.items():
            path = self.workflows / filename
            original = path.read_text(encoding="utf-8")
            jobs = CHECK.block_mapping(CHECK.block_mapping(original, 0)["jobs"][1], 2)
            for job_id in job_ids:
                body = jobs[job_id][1]
                self.assertIn(bootstrap, body)
                removed = body.replace(bootstrap, "", 1)
                mutations = {
                    "missing": removed,
                    "late": removed + "\n" + bootstrap,
                    "conditional": body.replace(bootstrap, bootstrap + "        if: false\n", 1),
                    "ignored-failure": body.replace(
                        bootstrap, bootstrap + "        continue-on-error: true\n", 1),
                }
                for name, mutation in mutations.items():
                    with self.subTest(workflow=filename, job=job_id, mutation=name):
                        path.write_text(original.replace(body, mutation, 1), encoding="utf-8")
                        self.assert_rejected(f"{job_id} must bootstrap Maven unconditionally")
                path.write_text(original, encoding="utf-8")

    def test_new_direct_maven_job_cannot_borrow_another_jobs_bootstrap(self):
        """Reject a new direct Maven step in each scoped Linux workflow.

        Earlier jobs already bootstrap, so appending an unprotected job also
        verifies that successful bootstrap state does not cross job boundaries.
        Only temporary fixtures change; they are restored after assertions
        and their directory is removed by tear-down even on failure.
        """
        for filename in MAVEN_JOBS:
            path = self.workflows / filename
            original = path.read_text(encoding="utf-8")
            with self.subTest(workflow=filename):
                path.write_text(original + "\n  another-maven-job:\n    runs-on: ubuntu-24.04\n"
                                "    steps:\n      - run: ./mvnw -B validate\n", encoding="utf-8")
                self.assert_rejected("another-maven-job must bootstrap Maven unconditionally")
            path.write_text(original, encoding="utf-8")

    def test_maven_bootstrap_guard_is_scoped_to_direct_linux_commands(self):
        """Accept composite-only jobs and ignore Maven outside the two workflows.

        Temporary fixtures add a composite caller and a job mentioning ./mvnw
        only in a shell comment and env value, plus another workflow with an
        unbootstrapped direct run. These need no direct Linux bootstrap; the
        existing five protected jobs must still pass. No repository files are
        changed and tear-down removes all added files even on assertion error.
        """
        for filename in MAVEN_JOBS:
            path = self.workflows / filename
            with path.open("a", encoding="utf-8") as stream:
                stream.write("\n  composite-only:\n    runs-on: ubuntu-24.04\n"
                             "    steps:\n      - uses: ./.github/actions/java-test\n"
                             "\n  no-direct-maven:\n    runs-on: ubuntu-24.04\n    steps:\n"
                             "      - run: |\n          # ./mvnw is owned by the composite.\n"
                             "          echo complete\n        env:\n          COMMAND: ./mvnw\n")
        (self.workflows / "unrelated.yml").write_text(
            "jobs:\n  other-maven:\n    steps:\n      - run: ./mvnw validate\n", encoding="utf-8")
        self.assertEqual(CHECK.linux_maven_bootstrap_failures(self.workflows), [])

    def test_maven_bootstrap_can_explicitly_propagate_failure(self):
        """Accept explicit continue-on-error false on Linux bootstrap steps.

        Mutate only temporary workflows to spell out the default failure
        behavior, then check the protected direct Maven callers still pass.
        Tear-down removes the fixtures after success or assertion failure.
        """
        for filename in MAVEN_JOBS:
            path = self.workflows / filename
            original = path.read_text(encoding="utf-8")
            action = f"        uses: {CHECK.MAVEN_BOOTSTRAP_ACTION}\n"
            self.assertIn(action, original)
            path.write_text(original.replace(action, action + "        continue-on-error: false\n"),
                            encoding="utf-8")
        self.assertEqual(CHECK.linux_maven_bootstrap_failures(self.workflows), [])


if __name__ == "__main__":
    unittest.main()
